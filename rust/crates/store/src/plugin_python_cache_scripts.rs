//! Exact bounded-result DjangoRedis7/redis-py6.4 dependency scripts.
//! Original django-redis and redis-py authors retain credit/licenses; pinned
//! collector must retain their actual distribution notices before publication.
//! No arbitrary user Lua or prefix-based script admission.
struct Script {
    text: &'static [u8],
    sha: &'static [u8],
    argc: usize,
}
const SCRIPTS: &[Script] = &[
    Script {
        text: br###"
                    local exists = redis.call('EXISTS', KEYS[1])
                    if (exists == 1) then
                        return redis.call('INCRBY', KEYS[1], ARGV[1])
                    else return false end
                    "###,
        sha: b"f4a7da3e5d8ea3b46642af08427151cae0190400",
        argc: 5,
    },
    Script {
        text: br###"
                    return redis.call('INCRBY', KEYS[1], ARGV[1])
                    "###,
        sha: b"4d15273b16b10c15e1d9202cbe93c959ab7d250b",
        argc: 5,
    },
    Script {
        text: br###"
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    "###,
        sha: b"c3f8721cbb97f72bc19e972846bd7aaf91901658",
        argc: 5,
    },
    Script {
        text: br###"
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        local expiration = redis.call('pttl', KEYS[1])
        if not expiration then
            expiration = 0
        end
        if expiration < 0 then
            return 0
        end

        local newttl = ARGV[2]
        if ARGV[3] == "0" then
            newttl = ARGV[2] + expiration
        end
        redis.call('pexpire', KEYS[1], newttl)
        return 1
    "###,
        sha: b"a4e8783852e6b949f9ef3a97212805108459a890",
        argc: 7,
    },
    Script {
        text: br###"
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('pexpire', KEYS[1], ARGV[2])
        return 1
    "###,
        sha: b"1cac51482acf5858da00f6d685d68f886cd6b6b2",
        argc: 6,
    },
];
pub(super) fn direct(command: &str, args: &[Vec<u8>]) -> bool {
    match command {
        "EVAL" => SCRIPTS
            .iter()
            .any(|s| args.len() == s.argc && args[1].as_slice() == s.text && args[2] == b"1"),
        "EVALSHA" => SCRIPTS
            .iter()
            .any(|s| args.len() == s.argc && args[1].as_slice() == s.sha && args[2] == b"1"),
        "SCRIPT" if args.len() == 3 && args[1].eq_ignore_ascii_case(b"LOAD") => {
            SCRIPTS.iter().any(|s| args[2].as_slice() == s.text)
        }
        "SCRIPT" if args.len() >= 3 && args[1].eq_ignore_ascii_case(b"EXISTS") => args[2..]
            .iter()
            .all(|sha| SCRIPTS.iter().any(|s| sha.as_slice() == s.sha)),
        _ => false,
    }
}
