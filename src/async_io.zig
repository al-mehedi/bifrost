//! **Asynchronous I/O API's on top of Linux `oi_uring` interface**

const std = @import("std");
const os = std.os;
const linux = os.linux;
const debug = std.debug;
const SemVer = std.SemanticVersion;

const IoUring = linux.IoUring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

const builtin = @import("builtin");

const stdx = @import("stdx.zig");

const Fifo = @import("fifo.zig").Fifo;


pub const Io = struct {
    ring: IoUring,

    /// Operations not yet submitted to the kernel
    /// Waiting on available space in the Submission Queue.
    unqueued: Fifo(Completion) = .{ .name = "io_unqueued" },

    /// Completions that are ready to have their callbacks run.
    completed: Fifo(Completion) = .{ .name = "io_completed" },

    ios_queued: u64 = 0,
    ios_in_kernel: u64 = 0,

    pub fn init(entries: u12, flags: u32) !Io {
        // Ensures that we support all `io_uring` ops used in this file.
        const uts = std.posix.uname();
        const version = try stdx.parseDirtySemver(&uts.release);
        const sem_ver = SemVer { .major = 5, .minor = 5, .patch = 0 };
        if (version.order(sem_ver) == .lt) {
            @panic("Linux kernel >= 5.5 is required for io_uring OP_ACCEPT");
        }

        return Io { .ring = try IoUring.init(entries, flags) };
    }

    pub fn deinit(self: *Io) void { self.ring.deinit(); }

    /// **Pass all queued submissions to the kernel and peek for completions**
    pub fn tick(self: *Io) !void {
        // We assume that all timeouts submitted by `runFor()` will be reaped by `runFor()` and that `tick()` and `runFor()` cannot be run concurrently. Therefore `timeouts` here will never be decremented and `etime` will always be false.
        var timeouts: usize = 0;
        var etime = false;

        try self.flush(0, &timeouts, &etime);
        debug.assert(etime == false);

        // Flush any SQEs that were queued while running completion callbacks in `flush()`. This is an optimization to avoid delaying submissions until the next tick. At the same time, we do not flush any ready CQEs since SQEs may complete synchronously.

        // We guard against an `io_uring_enter()` syscall if we know we do not have any queued SQEs.We cannot use `self.ring.sq_ready()` here since this counts flushed and unflushed SQEs.
        const queued = self.ring.sq.sqe_tail -% self.ring.sq.sqe_head;
        if (queued > 0) {
            try self.flush_submissions(0, &timeouts, &etime);
            debug.assert(etime == false);
        }
    }

    /// **Pass all queued submissions to the kernel and run for `nanoseconds`**
    /// - The `nanoseconds` argument is a **u63** to allow coercion
    /// - To the **i64** used in the kernel_timespec struct.
    pub fn runFor(self: *Io, nanoseconds: u63) !void {
        // We must use the same clock source (CLOCK_MONOTONIC) used by `io_uring` since we specify the timeout below as an absolute value. Otherwise, we may deadlock if the clock sources are dramatically different. Any kernel that supports `io_uring` will CLOCK_MONOTONIC.
        var current_ts: os.linux.timespec = undefined;
        const clock_monotonic = os.linux.CLOCK.MONOTONIC;
        os.linux.clock_gettime(clock_monotonic, &current_ts) catch unreachable;

        // The absolute CLOCK_MONOTONIC time
        // After which we may return from this function:
        const timeout_ts: os.linux.kernel_timespec = .{
            .tv_sec = current_ts.tv_sec,
            .tv_nsec = current_ts.tv_nsec + nanoseconds,
        };
        var timeouts: usize = 0;
        var etime = false;

        while (!etime) {
            const timeout_sqe = self.ring.get_sqe() catch blk: {
                // The submission queue is full
                // So flush submissions to make space:
                try self.flush_submissions(0, &timeouts, &etime);
                break :blk self.ring.get_sqe() catch unreachable;
            };

            // Submit an absolute timeout that will be canceled
            // If any other SQE completes first:
            linux.io_uring_prep_timeout(timeout_sqe, &timeout_ts, 1, os.linux.IORING_TIMEOUT_ABS);
            timeout_sqe.user_data = 0;
            timeouts += 1;

            // We don't really want to count this timeout as an io,
            // but it's tricky to track separately.
            // Is it still true if i'm not using plot tracing?
            self.ios_queued += 1;

            // The amount of time this call will block is bounded
            // By the timeout we just submitted:
            try self.flush(1, &timeouts, &etime);
        }

        // Reap any remaining timeouts, which reference the timespec in the current stack frame. The busy loop here is required to avoid a potential deadlock, as the kernel determines when the timeouts are pushed to the completion queue, not us.
        while (timeouts > 0) {
            _ = try self.flush_completions(0, &timeouts, &etime);
        }
    }

    fn flush(self: *Io, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        // Flush any queued SQEs
        // Reuse the same syscall to wait for completions if required:
        try self.flush_submissions(wait_nr, timeouts, etime);

        // We can now just peek for any CQEs
        // Without waiting and without another syscall:
        try self.flush_completions(0, timeouts, etime);

        // The SQE array is empty from flush_submissions(). Fill it up with unqueued completions. This runs before `self.completed` is flushed below to prevent new IO from reserving SQE slots and potentially starving those in `self.unqueued`. Loop over a copy to avoid an infinite loop of `enqueue()` re-adding to `self.unqueued`.
        {
            var copy = self.unqueued;
            self.unqueued.reset();
            while (copy.pop()) |completion| self.enqueue(completion);
        }

        // Run completions only after all completions have been flushed:
        // Loop until all completions are processed. Calls to complete() may queue more work and extend the duration of the loop, but this is fine as it (A.) executes completions that become ready without going through another syscall from `flush_submissions()` and (B.) potentially queues more SQEs to take advantage more of the next `flush_submissions()`.
        while (self.completed.pop()) |completion| {
            completion.complete(&self.callback_tracer_slot);
        }

        // At this point, unqueued could have completions either by (A.) those who didn't get an SQE during the popping of unqueued or (B.) `completion.complete()` which start new IO. These unqueued completions will get priority to acquiring SQEs on the next `flush()`.
    }

    // TODO: Refactor from here
    fn flush_completions(self: *Io, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        var cqes: [256]io_uring_cqe = undefined;
        var wait_remaining = wait_nr;
        while (true) {
            // Guard against waiting indefinitely (if there are too few requests inflight),
            // especially if this is not the first time round the loop:
            const completed = self.ring.copy_cqes(&cqes, wait_remaining) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            if (completed > wait_remaining) wait_remaining = 0 else wait_remaining -= completed;
            for (cqes[0..completed]) |cqe| {
                self.ios_in_kernel -= 1;

                if (cqe.user_data == 0) {
                    timeouts.* -= 1;
                    // We are only done if the timeout submitted was completed due to time, not if
                    // it was completed due to the completion of an event, in which case `cqe.res`
                    // would be 0. It is possible for multiple timeout operations to complete at the
                    // same time if the nanoseconds value passed to `runFor()` is very short.
                    if (-cqe.res == @intFromEnum(std.os.E.TIME)) etime.* = true;
                    continue;
                }
                const completion: *Completion = @ptrFromInt(cqe.user_data);
                completion.result = cqe.res;
                // We do not run the completion here (instead appending to a linked list) to avoid:
                // * recursion through `flush_submissions()` and `flush_completions()`,
                // * unbounded stack usage, and
                // * confusing stack traces.
                self.completed.push(completion);
            }

            // tracer.plot(
            //     .{ .queue_count = .{ .queue_name = "io_in_kernel" } },
            //     @as(f64, @floatFromInt(self.ios_in_kernel)),
            // );

            if (completed < cqes.len) break;
        }
    }

    fn flush_submissions(self: *Io, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        while (true) {
            const submitted = self.ring.submit_and_wait(wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                // Wait for some completions and then try again:
                // See https://github.com/axboe/liburing/issues/281 re: error.SystemResources.
                // Be careful also that copy_cqes() will flush before entering to wait (it does):
                // https://github.com/axboe/liburing/commit/35c199c48dfd54ad46b96e386882e7ac341314c5
                error.CompletionQueueOvercommitted, error.SystemResources => {
                    try self.flush_completions(1, timeouts, etime);
                    continue;
                },
                else => return err,
            };

            self.ios_queued -= submitted;
            self.ios_in_kernel += submitted;
            // tracer.plot(
            //     .{ .queue_count = .{ .queue_name = "io_queued" } },
            //     @as(f64, @floatFromInt(self.ios_queued)),
            // );
            // tracer.plot(
            //     .{ .queue_count = .{ .queue_name = "io_in_kernel" } },
            //     @as(f64, @floatFromInt(self.ios_in_kernel)),
            // );

            break;
        }
    }

    fn enqueue(self: *Io, completion: *Completion) void {
        const sqe = self.ring.get_sqe() catch |err| switch (err) {
            error.SubmissionQueueFull => {
                self.unqueued.push(completion);
                return;
            },
        };
        completion.prep(sqe);

        self.ios_queued += 1;
        // tracer.plot(
        //     .{ .queue_count = .{ .queue_name = "io_queued" } },
        //     @as(f64, @floatFromInt(self.ios_queued)),
        // );
    }

    /// This struct holds the data needed for a single io_uring operation
    pub const Completion = struct {
        io: *Io,
        result: i32 = undefined,
        next: ?*Completion = null,
        operation: Operation,
        context: ?*anyopaque,
        callback: *const fn (
            context: ?*anyopaque,
            completion: *Completion,
            result: *const anyopaque,
        ) void,

        fn prep(completion: *Completion, sqe: *io_uring_sqe) void {
            switch (completion.operation) {
                .accept => |*op| {
                    linux.io_uring_prep_accept(
                        sqe,
                        op.socket,
                        &op.address,
                        &op.address_size,
                        std.os.SOCK.CLOEXEC,
                    );
                },
                .close => |op| {
                    linux.io_uring_prep_close(sqe, op.fd);
                },
                .connect => |*op| {
                    linux.io_uring_prep_connect(
                        sqe,
                        op.socket,
                        &op.address.any,
                        op.address.getOsSockLen(),
                    );
                },
                .read => |op| {
                    linux.io_uring_prep_read(
                        sqe,
                        op.fd,
                        op.buffer[0..bufferLimit(op.buffer.len)],
                        op.offset,
                    );
                },
                .recv => |op| {
                    linux.io_uring_prep_recv(sqe, op.socket, op.buffer, std.os.MSG.NOSIGNAL);
                },
                .send => |op| {
                    linux.io_uring_prep_send(sqe, op.socket, op.buffer, std.os.MSG.NOSIGNAL);
                },
                .timeout => |*op| {
                    linux.io_uring_prep_timeout(sqe, &op.timespec, 0, 0);
                },
                .write => |op| {
                    linux.io_uring_prep_write(
                        sqe,
                        op.fd,
                        op.buffer[0..bufferLimit(op.buffer.len)],
                        op.offset,
                    );
                },
            }
            sqe.user_data = @intFromPtr(completion);
        }

        fn complete(completion: *Completion) void {
            switch (completion.operation) {
                .accept => {
                    const result: anyerror!os.linux.socket_t = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNABORTED => error.ConnectionAborted,
                                .FAULT => unreachable,
                                .INVAL => error.SocketNotListening,
                                .MFILE => error.ProcessFdQuotaExceeded,
                                .NFILE => error.SystemFdQuotaExceeded,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PERM => error.PermissionDenied,
                                .PROTO => error.ProtocolFailure,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    call_callback(completion, &result);
                },
                .close => {
                    const result: anyerror!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                // A success, see https://github.com/ziglang/zig/issues/2425
                                .INTR => {},
                                .BADF => error.FileDescriptorInvalid,
                                .DQUOT => error.DiskQuota,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            debug.assert(completion.result == 0);
                        }
                    };
                    call_callback(completion, &result);
                },
                .connect => {
                    const result: anyerror!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .ADDRINUSE => error.AddressInUse,
                                .ADDRNOTAVAIL => error.AddressNotAvailable,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .AGAIN, .INPROGRESS => error.WouldBlock,
                                .ALREADY => error.OpenAlreadyInProgress,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNREFUSED => error.ConnectionRefused,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .ISCONN => error.AlreadyConnected,
                                .NETUNREACH => error.NetworkUnreachable,
                                .NOENT => error.FileNotFound,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .PERM => error.PermissionDenied,
                                .PROTOTYPE => error.ProtocolNotSupported,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            debug.assert(completion.result == 0);
                        }
                    };
                    call_callback(completion, &result);
                },
                .read => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.NotOpenForReading,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .ISDIR => error.IsDir,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .SPIPE => error.Unseekable,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    call_callback(completion, &result);
                },
                .recv => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNREFUSED => error.ConnectionRefused,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                .OPNOTSUPP => error.OperationNotSupported,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    call_callback(completion, &result);
                },
                .send => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .AGAIN => error.WouldBlock,
                                .ALREADY => error.FastOpenAlreadyInProgress,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .DESTADDRREQ => unreachable,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .ISCONN => unreachable,
                                .MSGSIZE => error.MessageTooBig,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PIPE => error.BrokenPipe,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    call_callback(completion, &result);
                },
                .timeout => {
                    debug.assert(completion.result < 0);
                    const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                        .INTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        .CANCELED => error.Canceled,
                        .TIME => {}, // A success.
                        else => |errno| os.unexpectedErrno(errno),
                    };
                    const result: anyerror!void = err;
                    call_callback(completion, &result);
                },
                .write => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.NotOpenForWriting,
                                .DESTADDRREQ => error.NotConnected,
                                .DQUOT => error.DiskQuota,
                                .FAULT => unreachable,
                                .FBIG => error.FileTooBig,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .PERM => error.AccessDenied,
                                .PIPE => error.BrokenPipe,
                                .SPIPE => error.Unseekable,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    call_callback(completion, &result);
                },
            }
        }
    };

    fn call_callback(completion: *Completion, result: *const anyopaque) void {
        // tracer.start(
        //     callback_tracer_slot,
        //     .io_callback,
        //     @src(),
        // );
        completion.callback(completion.context, completion, result);
        // tracer.end(
        //     callback_tracer_slot,
        //     .io_callback,
        // );
    }

    /// This union encodes the set of operations supported as well as their arguments.
    const Operation = union(enum) {
        accept: struct {
            socket: os.linux.socket_t,
            address: os.linux.sockaddr = undefined,
            address_size: os.linux.socklen_t = @sizeOf(os.linux.sockaddr),
        },
        close: struct {
            fd: os.linux.fd_t,
        },
        connect: struct {
            socket: os.linux.socket_t,
            address: std.net.Address,
        },
        read: struct {
            fd: os.linux.fd_t,
            buffer: []u8,
            offset: u64,
        },
        recv: struct {
            socket: os.linux.socket_t,
            buffer: []u8,
        },
        send: struct {
            socket: os.linux.socket_t,
            buffer: []const u8,
        },
        timeout: struct {
            timespec: os.linux.kernel_timespec,
        },
        write: struct {
            fd: os.linux.fd_t,
            buffer: []const u8,
            offset: u64,
        },
    };

    pub const AcceptError = error {
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionAborted,
        SocketNotListening,
        ProcessFdQuotaExceeded,
        SystemFdQuotaExceeded,
        SystemResources,
        FileDescriptorNotASocket,
        OperationNotSupported,
        PermissionDenied,
        ProtocolFailure,
    } || os.UnexpectedError;

    pub fn accept(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!os.linux.socket_t,
        ) void,
        completion: *Completion,
        socket: os.linux.socket_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const AcceptError!os.linux.socket_t, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .accept = .{
                    .socket = socket,
                    .address = undefined,
                    .address_size = @sizeOf(os.linux.sockaddr),
                },
            },
        };
        self.enqueue(completion);
    }

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || os.UnexpectedError;

    pub fn close(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: os.linux.fd_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const CloseError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .close = .{ .fd = fd },
            },
        };
        self.enqueue(completion);
    }

    pub const ConnectError = error {
        AccessDenied,
        AddressInUse,
        AddressNotAvailable,
        AddressFamilyNotSupported,
        WouldBlock,
        OpenAlreadyInProgress,
        FileDescriptorInvalid,
        ConnectionRefused,
        AlreadyConnected,
        NetworkUnreachable,
        FileNotFound,
        FileDescriptorNotASocket,
        PermissionDenied,
        ProtocolNotSupported,
        ConnectionTimedOut,
        SystemResources,
    } || os.UnexpectedError;

    pub fn connect(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ConnectError!void,
        ) void,
        completion: *Completion,
        socket: os.linux.socket_t,
        address: std.net.Address,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const ConnectError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .connect = .{
                    .socket = socket,
                    .address = address,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
        ConnectionTimedOut,
    } || os.UnexpectedError;

    pub fn read(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: os.linux.fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const ReadError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const RecvError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionRefused,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        ConnectionTimedOut,
        OperationNotSupported,
    } || os.UnexpectedError;

    pub fn recv(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: RecvError!usize,
        ) void,
        completion: *Completion,
        socket: os.linux.socket_t,
        buffer: []u8,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const RecvError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const SendError = error{
        AccessDenied,
        WouldBlock,
        FastOpenAlreadyInProgress,
        AddressFamilyNotSupported,
        FileDescriptorInvalid,
        ConnectionResetByPeer,
        MessageTooBig,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        OperationNotSupported,
        BrokenPipe,
        ConnectionTimedOut,
    } || os.UnexpectedError;

    pub fn send(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: SendError!usize,
        ) void,
        completion: *Completion,
        socket: os.linux.socket_t,
        buffer: []const u8,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const SendError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const TimeoutError = error{Canceled} || os.UnexpectedError;

    pub fn timeout(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const TimeoutError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .timeout = .{
                    .timespec = .{ .tv_sec = 0, .tv_nsec = nanoseconds },
                },
            },
        };

        // Special case a zero timeout as a yield.
        if (nanoseconds == 0) {
            completion.result = -@as(i32, @intFromEnum(std.os.E.TIME));
            self.completed.push(completion);
            return;
        }

        self.enqueue(completion);
    }

    pub const WriteError = error{
        WouldBlock,
        NotOpenForWriting,
        NotConnected,
        DiskQuota,
        FileTooBig,
        Alignment,
        InputOutput,
        NoSpaceLeft,
        Unseekable,
        AccessDenied,
        BrokenPipe,
    } || os.UnexpectedError;

    pub fn write(
        self: *Io,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: os.linux.fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const WriteError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
    }
};

fn bufferLimit(buffer_len: usize) usize {
    // On Linux, `write()` (and similar system calls) will transfer at most `0x7ffff000` (2,147,479,552) bytes, returning the number of bytes actually transferred. (This is true on both 32-bit and 64-bit systems.) Due to using a signed C int as the return value, as well as stuffing the errno codes into the last `4096` values.

    // According to POSIX.1, if count is greater than `SSIZE_MAX`, the result is implementation-defined. Darwin limits writes to `0x7fffffff` bytes, more than that returns `EINVAL`. Read this article - https://stackoverflow.com/questions/70368651/why-cant-linux-write-more-than-2147479552-bytes

    // The corresponding POSIX limit is `std.math.maxInt(isize)`.
    const limit = switch (builtin.target.os.tag) {
        .linux => 0x7ffff000,
        .macos, .ios, .watchos, .tvos => std.math.maxInt(i32),
        else => std.math.maxInt(isize),
    };
    return @min(limit, buffer_len);
}
