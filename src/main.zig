const std = @import("std");

pub fn main() void {
    const sum = add(50, 1);
    std.debug.print("👽 - Area {}!\n", .{sum});
}

fn add(x: i32, y: i32) i32 {
    return x + y;
}

test "Awe-sum" {
    const sum = add(11, 8);
    try std.testing.expect(sum == 19);
}
