//! **An intrusive First-In/First-Out linked list**
//! https://www.data-structures-in-practice.com/intrusive-linked-lists

const std = @import("std");
const debug = std.debug;


/// The element type `T` must have a field called **next** of type `?*T`.
/// The **next** field will point to the next element of the linked list.
pub fn Fifo(comptime T: type) type {
    return struct {
        const Self = @This();

        in: ?*T = null,
        out: ?*T = null,
        count: u64 = 0,

        // Should only be null if we don't want to monitor `count`.
        name: ?[]const u8,

        pub fn push(self: *Self, elm: *T) void {
            debug.assert(self.contains(elm) == false);
            debug.assert(elm.next == null);

            if (self.in) |in| {
                in.next = elm;
                self.in = elm;
            } else {
                debug.assert(self.out == null);
                self.in = elm;
                self.out = elm;
            }

            self.count += 1;
        }

        pub fn pop(self: *Self) ?*T {
            const ret = self.out orelse return null;
            self.out = ret.next;
            ret.next = null;
            if (self.in == ret) self.in = null;
            self.count -= 1;
            return ret;
        }

        pub fn peekLast(self: Self) ?*T { return self.in; }

        pub fn peek(self: Self) ?*T { return self.out; }

        pub fn empty(self: Self) bool { return self.peek() == null; }

        /// Returns whether the linked list contains the given *exact element*.
        /// Does **pointer comparison** for equality checks.
        pub fn contains(self: *const Self, elm_needle: *const T) bool {
            var iterator = self.peek();
            while (iterator) |elm| : (iterator = elm.next) {
                if (elm == elm_needle) return true;
            }
            return false;
        }

        /// Asserts that the element is in the FIFO. This operation is `O(N)`.
        /// If this is done often you probably want a different data structure.
        pub fn remove(self: *Self, to_remove: *T) void {
            if (to_remove == self.out) {
                _ = self.pop();
                return;
            }

            var it = self.out;
            while (it) |elm| : (it = elm.next) {
                if (to_remove == elm.next) {
                    if (to_remove == self.in) self.in = elm;
                    elm.next = to_remove.next;
                    to_remove.next = null;
                    self.count -= 1;
                    break;
                }
            } else unreachable;
        }

        pub fn reset(self: *Self) void { self.* = .{ .name = self.name }; }
    };
}

test "Fifo: push/pop/peek/remove/empty" {
    const testing = @import("std").testing;

    const Foo = struct { value: u8, next: ?*@This() = null };

    var one: Foo = .{ .value = 1 };
    var two: Foo = .{ .value = 2 };
    var three: Foo = .{ .value = 3 };

    var fifo: Fifo(Foo) = .{ .name = null };
    try testing.expect(fifo.empty());

    fifo.push(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&two);
    fifo.push(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.peek());
    try testing.expect(fifo.contains(&one));
    try testing.expect(fifo.contains(&two));
    try testing.expect(fifo.contains(&three));

    fifo.remove(&one);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &two), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expect(!fifo.contains(&one));
    try testing.expect(!fifo.contains(&two));
    try testing.expect(!fifo.contains(&three));

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&two);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.push(&three);
    fifo.remove(&three);
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expect(!fifo.empty());
    try testing.expectEqual(@as(?*Foo, &two), fifo.pop());
    try testing.expect(fifo.empty());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&one);
    fifo.push(&two);
    fifo.remove(&two);
    fifo.push(&three);
    try testing.expectEqual(@as(?*Foo, &one), fifo.pop());
    try testing.expectEqual(@as(?*Foo, &three), fifo.pop());
    try testing.expectEqual(@as(?*Foo, null), fifo.pop());
    try testing.expect(fifo.empty());

    fifo.push(&two);
    const item = fifo.pop();
    try testing.expect(fifo.empty());
    try testing.expect(item.?.*.value == 2);
}