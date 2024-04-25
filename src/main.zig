const std = @import("std");
const debug = std.debug;
const Tag = std.Target.Os.Tag;

const stdx = @import("stdx.zig");

const async_io = @import("async_io.zig");
const Aio = async_io.Io;





pub fn main() !void {
    stdx.targetPlatform(Tag.linux);

    const x = try Aio.init(8, 0);
    _ = x;
}

