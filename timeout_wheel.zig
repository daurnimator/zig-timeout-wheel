// Ported from https://github.com/wahern/timeout/

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const rotl = std.math.rotl;
const rotr = std.math.rotr;

fn fls(n: var) usize {
    return @typeOf(n).bit_count - @clz(n);
}


/// wheel_bit - The number of value bits mapped in each wheel. The
///             lowest-order wheel_bit bits index the lowest-order (highest
///             resolution) wheel, the next group of wheel_bit bits the
///             higher wheel, etc.
/// wheel_num - The number of wheels. wheel_bit * wheel_num = the number of
///             value bits used by all the wheels. Any timeout value
///             larger than this will cycle through again.
fn TimeoutWheel(comptime timeout_t: type, wheel_bit:comptime_int, wheel_num:comptime_int, allow_intervals:bool, allow_relative_access:bool) type {
    const abstime_t = timeout_t;
    const reltime_t = timeout_t;

    assert(wheel_bit>0);
    assert(wheel_num>0);
    assert(((1 << (wheel_bit * wheel_num)) - 1) <= std.math.maxInt(timeout_t));

    const wheel_t = @IntType(false, 1<<wheel_bit);
    const wheel_len = (1 << wheel_bit);
    const wheel_max = (wheel_len - 1);
    const wheel_mask = (wheel_len - 1);
    const wheel_num_t = @IntType(false, wheel_num);
    const wheel_slot_t = @IntType(false, wheel_bit);

    return struct {
        const Self = @This();
        const TimeoutWheelType = Self;

        const TimeoutType = timeout_t;

        const TimeoutData = struct {
            /// initialize TimeoutData structure
            pub fn init(init_interval:?reltime_t) TimeoutData {
                return TimeoutData {
                    .expires = 0,
                    .pending = null,
                    .interval = init: {
                        if (allow_intervals) {
                            if (init_interval) |int| {
                                assert(int > 0);
                                break :init int;
                            } else {
                                break :init 0;
                            }
                        } else {
                            assert(init_interval == null);
                        }
                    },
                    .timeouts = if (allow_relative_access) null,
                };
            }

            /// absolute expiration time
            expires: abstime_t,

            /// TimeoutData list if pending on wheel or expiry queue
            pending: ?*TimeoutList,

            /// TimeoutData interval if periodic
            /// rather than using an optional type we internally use 0 to indicate no interval
            interval: if (allow_intervals) reltime_t else void,

            /// timeouts collection if member of
            timeouts: if (allow_relative_access) ?*TimeoutWheelType else void,
            fn setTimeouts(self: *TimeoutData, T: ?*TimeoutWheelType) void {
                if (allow_relative_access) {
                    self.timeouts = T;
                }
            }
        };

        const TimeoutList = std.LinkedList(TimeoutData);

        // https://github.com/ziglang/zig/pull/1736
        /// Concatenate list2 onto the end of list1, removing all entries from the former.
        ///
        /// Arguments:
        ///     list1: the list to concatenate onto
        ///     list2: the list to be concatenated
        fn TimeoutListConcat(list1: *TimeoutList, list2: *TimeoutList) void {
            if (list2.first) |l2_first| {
                if (list1.last) |l1_last| {
                    l1_last.next = list2.first;
                    l2_first.prev = list1.last;
                    list1.len += list2.len;
                } else {
                    // list1 was empty
                    list1.first = list2.first;
                    list1.len = list2.len;
                }
                list1.last = list2.last;
                list2.first = null;
                list2.last = null;
                list2.len = 0;
            }
        }


        /// Public Timeout structure
        pub const Timeout = struct {
            node: TimeoutList.Node,

            /// true if on timing wheel, false otherwise
            pub fn isPending(self: *Timeout) if (allow_relative_access) bool else void {
                if (allow_relative_access) {
                    if (self.node.data.pending) |p| {
                        return p != &self.node.data.timeouts.?.expiredList;
                    }
                    return false;
                }
            }

            /// true if on expired queue, false otherwise
            pub fn isExpired(self: *Timeout) if (allow_relative_access) bool else void {
                if (allow_relative_access) {
                    if (self.node.data.pending) |p| {
                        return p == &self.node.data.timeouts.?.expiredList;
                    }
                    return false;
                }
            }

            /// remove timeout from any timing wheel (okay if not member of any)
            pub fn delete(self: *Timeout) void {
                if (allow_relative_access) {
                    self.node.data.timeouts.?.delete(self);
                }
            }
        };

        /// Allocate and initialize a Timeout and its data.
        ///
        /// Arguments:
        ///     allocator: Dynamic memory allocator.
        ///
        /// Returns:
        ///     A pointer to the new timeout.
        pub fn createTimeout(self: *Self, interval:?reltime_t, allocator: *Allocator) !*Timeout {
            return allocator.create(Timeout{.node = TimeoutList.Node.init(TimeoutData.init(interval))});
        }

        /// Deallocate a Timeout.
        ///
        /// Arguments:
        ///     Timeout: Pointer to the Timeout to deallocate.
        ///     allocator: Dynamic memory allocator.
        pub fn destroyTimeout(self: *Self, to: *Timeout, allocator: *Allocator) void {
            allocator.destroy(to);
        }

        wheel: [wheel_num][wheel_len]TimeoutList,
        expiredList: TimeoutList,
        pendingWheels: [wheel_num]wheel_t,
        curtime: abstime_t,

        pub fn init() Self {
            return Self{
                .wheel = init: {
                    var initial_value: [wheel_num][wheel_len]TimeoutList = undefined;
                    for (initial_value) |*pt| {
                        pt.* = nested: {
                            var nested_value: [wheel_len]TimeoutList = undefined;
                            for (nested_value) |*npt| {
                                npt.* = TimeoutList.init();
                            }
                            break :nested nested_value;
                        };
                    }
                    break :init initial_value;
                },
                .expiredList = TimeoutList.init(),
                .pendingWheels = []wheel_t{0} ** wheel_num,
                .curtime = 0,
            };
        }

        pub fn reset(self: *Self) void {
            var resetList = TimeoutList.init();

            for (self.wheel) |*wheel| {
                for (wheel) |*slot_ptr| {
                    TimeoutListConcat(&resetList, slot_ptr);
                }
            }

            TimeoutListConcat(&resetList, &self.expiredList);

            {
                var it = resetList.first;
                while (it) |node| : (it = node.next) {
                    var td:*TimeoutData = &node.data;
                    td.pending = null;
                    td.setTimeouts(null);
                }
            }
        }

        pub fn delete(self: *Self, to: *Timeout) void {
            const td = &to.node.data;
            if (td.pending) |td_pending| {
                td_pending.remove(&to.node);

                if ((td_pending != &self.expiredList) and (td_pending.first == null)) {
                    // TODO: use pointer subtraction. See https://github.com/ziglang/zig/issues/1738
                    var index = @ptrToInt(td_pending) - @ptrToInt(&self.wheel[0][0]);
                    var wheel = @truncate(wheel_num_t, index / wheel_len);
                    var slot = @truncate(wheel_slot_t, index % wheel_len);

                    self.pendingWheels[wheel] &= ~(wheel_t(1) << slot);
                }

                td.pending = null;
                td.setTimeouts(null);
            }
        }

        fn timeout_rem(self: *const Self, td: *TimeoutData) reltime_t {
            return td.expires - self.curtime;
        }

        fn timeout_wheel(t: timeout_t) wheel_num_t {
            assert(t > 0); // must be called with timeout != 0, so fls input is nonzero
            return @intCast(wheel_num_t, (@intCast(std.math.Log2Int(timeout_t), fls(std.math.min(t, std.math.maxInt(timeout_t)))) - 1) / wheel_bit);
        }

        fn timeout_slot(wheel:wheel_num_t, expires: timeout_t) wheel_slot_t {
            assert(wheel < wheel_num);
            return @truncate(wheel_slot_t,
                (expires >> (std.math.Log2Int(timeout_t)(wheel) * wheel_bit))
                - if (wheel != 0) u1(1) else u1(0)
            );
        }

        fn sched(self: *Self, to: *Timeout, expires: timeout_t) void {
            self.delete(to);
            const td = &to.node.data;
            td.expires = expires;

            td.setTimeouts(self);

            if (expires > self.curtime) {
                var rem = self.timeout_rem(td);

                // rem is nonzero since:
                //   rem == timeout_rem(T, td),
                //       == td.expires - self.curtime
                //   and above we have expires > self.curtime.
                var wheel = timeout_wheel(rem);
                var slot = timeout_slot(wheel, td.expires);

                td.pending = &self.wheel[wheel][slot];
                td.pending.?.append(&to.node);

                self.pendingWheels[wheel] |= wheel_t(1) << slot;
            } else {
                td.pending = &self.expiredList;
                td.pending.?.append(&to.node);
            }
        }

        /// add timeout to timing wheel
        pub fn add(self: *Self, to: *Timeout, ticks: timeout_t) void {
            self.sched(to, self.curtime + ticks);
        }

        /// update timing wheel with current absolute time
        pub fn update(self: *Self, curtime: abstime_t) void {
            var elapsed:abstime_t = curtime - self.curtime;
            var todo = TimeoutList.init();

            var wheel:wheel_num_t = 0;
            while (wheel < wheel_num) : (wheel += 1 ) {
                var pending_slots:wheel_t = undefined;

                // Calculate the slots expiring in this wheel
                //
                // If the elapsed time is greater than the maximum period of
                // the wheel, mark every position as expiring.
                //
                // Otherwise, to determine the expired slots fill in all the
                // bits between the last slot processed and the current
                // slot, inclusive of the last slot. We'll bitwise-AND this
                // with our pending set below.
                //
                // If a wheel rolls over, force a tick of the next higher
                // wheel.
                const wheel_offset = std.math.Log2Int(abstime_t)(wheel) * wheel_bit;
                if ((elapsed >> wheel_offset) > wheel_max) {
                    pending_slots = ~wheel_t(0);
                } else {
                    var _elapsed = @truncate(wheel_slot_t, elapsed >> wheel_offset);

                    var oslot = self.curtime >> wheel_offset;
                    // https://github.com/ziglang/zig/issues/1739
                    pending_slots = rotl(wheel_t, (wheel_t(1) << _elapsed) - 1, @intCast(wheel_t, oslot));

                    var nslot = @truncate(wheel_slot_t, curtime >> wheel_offset);
                    // https://github.com/ziglang/zig/issues/1739
                    pending_slots |= wheel_t(1) << nslot;
                }

                while ((pending_slots & self.pendingWheels[wheel]) != 0) {
                    // ctz input cannot be zero: loop condition.
                    var slot = @truncate(wheel_slot_t, @ctz(pending_slots & self.pendingWheels[wheel]));
                    TimeoutListConcat(&todo, &self.wheel[wheel][slot]);
                    self.pendingWheels[wheel] &= ~(wheel_t(1) << slot);
                }

                if ((0x1 & pending_slots) == 0)
                    break; // break if we didn't wrap around end of wheel

                // if we're continuing, the next wheel must tick at least once
                elapsed = std.math.max(elapsed, abstime_t(wheel_len) << wheel_offset);
            }

            self.curtime = curtime;

            while (todo.first) |node| {
                var to = @fieldParentPtr(Timeout, "node", node);
                var td:*TimeoutData = &node.data;

                todo.remove(node);
                td.pending = null;

                self.sched(to, td.expires);
            }
        }

        /// step timing wheel by relative time
        pub fn step(self: *Self, elapsed: reltime_t) void {
            self.update(self.curtime + elapsed);
        }

        // return true if any timeouts pending on timing wheel
        pub fn pending(self: *const Self) bool {
            var pending_slots:wheel_t = 0;

            var wheel:wheel_num_t = 0;
            while (wheel < wheel_num) : (wheel += 1) {
                pending_slots |= self.pendingWheels[wheel];
            }

            return pending_slots != 0;
        }

        /// return true if any timeouts on expired queue
        pub fn expired(self: *const Self) bool {
            return self.expiredList.first != null;
        }

        /// Calculate the interval before needing to process any timeouts pending on
        /// any wheel.
        ///
        /// This might return a timeout value sooner than any installed timeout if
        /// only higher-order wheels have timeouts pending. We can only know when to
        /// process a wheel, not precisely when a timeout is scheduled. Our timeout
        /// accuracy could be off by 2^(N*M)-1 units where N is the wheel number and
        /// M is wheel_bit. Only timeouts which have fallen through to wheel 0 can be
        /// known exactly.
        ///
        /// We never return a timeout larger than the lowest actual timeout.
        pub fn timeout(self: *const Self) reltime_t {
            if (self.expiredList.first != null) {
                return 0;
            }

            var interval = ~timeout_t(0);
            var relmask:timeout_t = 0;

            var wheel:wheel_num_t = 0;
            while (wheel < wheel_num) : (wheel += 1) {
                if (self.pendingWheels[wheel] != 0) {
                    const slot = @truncate(wheel_slot_t, self.curtime >> (wheel * wheel_bit));

                    var _timeout:timeout_t = undefined;

                    {
                        // ctz input cannot be zero: self.pending[wheel] is
                        // nonzero, so rotr() is nonzero.
                        // https://github.com/ziglang/zig/issues/1739
                        var tmp = @ctz(rotr(wheel_t, self.pendingWheels[wheel], wheel_t(slot)));
                        // +1 to higher order wheels as those timeouts are one rotation in the future (otherwise they'd be on a lower wheel or expired)
                        _timeout = timeout_t(tmp + if(wheel != 0) u1(1) else u1(0)) << (wheel * wheel_bit);
                    }

                    _timeout -= relmask & self.curtime;
                    // reduce by how much lower wheels have progressed

                    interval = std.math.min(_timeout, interval);
                }

                relmask <<= wheel_bit;
                relmask |= wheel_mask;
            }

            return interval;
        }

        /// return any expired timeout (caller should loop until NULL-return)
        pub fn get(self: *Self) ?*Timeout {
            const node = self.expiredList.first orelse return null;
            const td:*TimeoutData = &(node).data;

            self.expiredList.remove(node);
            td.pending = null;
            td.setTimeouts(null);

            const to = @fieldParentPtr(Timeout, "node", node);

            if (allow_intervals and td.interval != 0) {
                td.expires += td.interval;

                if (td.expires <= self.curtime) {
                    // If we've missed the next firing of this timeout, reschedule
                    // it to occur at the next multiple of its interval after
                    // the last time that it fired.
                    var n = self.curtime - td.expires;
                    var r:timeout_t = n % td.interval;
                    td.expires = self.curtime + (td.interval - r);
                }

                self.sched(to, td.expires);
            }

            return to;
        }
    };
}

const DefaultTimeoutWheel = TimeoutWheel(u64, 6, 4, true, true);

test "initialization" {
    var t = DefaultTimeoutWheel.init();
}

test "timeout_wheel" {
    assert(DefaultTimeoutWheel.timeout_wheel(1) == 0);
    assert(DefaultTimeoutWheel.timeout_wheel(1<<6) == 1);
    assert(DefaultTimeoutWheel.timeout_wheel(1<<12) == 2);
    assert(DefaultTimeoutWheel.timeout_wheel(1<<18) == 3);
    assert(DefaultTimeoutWheel.timeout_wheel(1<<24) == 4);
    assert(DefaultTimeoutWheel.timeout_wheel(1<<32) == 5);
    assert(DefaultTimeoutWheel.timeout_wheel(1<<38) == 6);
}

test "simple test" {
    const allocator = std.debug.global_allocator;

    inline for ([]type{
        DefaultTimeoutWheel,
        // test with all flag combinations
        TimeoutWheel(u64, 6, 4, false, true),
        TimeoutWheel(u64, 6, 4, true, false),
        TimeoutWheel(u64, 6, 4, false, false),
        // test with some different bit sizes
        TimeoutWheel(u128, 11, 4, true, true),
        TimeoutWheel(u64, 10, 4, true, true),
        TimeoutWheel(u64, 9, 4, true, true),
        TimeoutWheel(u64, 8, 4, true, true),
        TimeoutWheel(u64, 7, 4, true, true),
        TimeoutWheel(u64, 5, 4, true, true),
        TimeoutWheel(u64, 4, 4, true, true),
        TimeoutWheel(u64, 3, 4, true, true),
        // different timeout sizes
        TimeoutWheel(u128, 3, 4, true, true),
        TimeoutWheel(u32, 3, 4, true, true),
        TimeoutWheel(u16, 3, 4, true, true),
        TimeoutWheel(u8, 2, 2, true, true),
    }) |TimeoutWheelType| {
        var mywheel = TimeoutWheelType.init();
        assert(mywheel.pending() == false);
        assert(mywheel.expired() == false);
        assert(mywheel.timeout() >= 0);
        var mytimeout = try mywheel.createTimeout(null, allocator);
        defer mywheel.destroyTimeout(mytimeout, allocator);
        mywheel.add(mytimeout, 5);
        assert(mywheel.pending() == true);
        assert(mywheel.expired() == false);
        assert(mywheel.timeout() <= 5);
        assert(mywheel.get() == null);
        mywheel.step(1);
        assert(mywheel.pending() == true);
        assert(mywheel.expired() == false);
        assert(mywheel.timeout() <= 4);
        assert(mywheel.get() == null);
        // step to one step before the timer should fire
        mywheel.step(3);
        assert(mywheel.pending() == true);
        assert(mywheel.expired() == false);
        assert(mywheel.timeout() == 1);
        assert(mywheel.get() == null);
        mywheel.step(1);
        assert(mywheel.pending() == false);
        assert(mywheel.expired() == true);
        assert(mywheel.timeout() == 0);
        assert(mywheel.get() == mytimeout);
        assert(mywheel.pending() == false);
        assert(mywheel.expired() == false);
        assert(mywheel.timeout() >= 0);
        assert(mywheel.get() == null);
    }
}
