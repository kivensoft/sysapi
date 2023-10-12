//! MD5 Crypt(3) 自定义MD5加密算法实现

use anyhow::Result;
use md5::{Md5, Digest};
use rand::Rng;

const SALT_LEN: usize = 8;
const DIGEST_LEN: usize = 22;
const SALT_MAGIC: &str = "$74$";
const DIGEST_OFFSET: usize = SALT_MAGIC.len() + SALT_LEN + 1;
const PWD_LEN: usize = DIGEST_OFFSET + DIGEST_LEN;

const CRYPT_B64_CHARS: &[u8] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// 口令加密
pub fn encrypt(password: &str) -> Result<String> {
    let mut salt_base64 = [0; SALT_LEN];

    gensalt(&mut salt_base64);

    let mut pass_out = [0; PWD_LEN];
    do_encrypt(&mut pass_out, password.as_bytes(), &salt_base64);

    Ok(String::from(std::str::from_utf8(&pass_out)?))
}

/// 口令校验
pub fn verify(pw_plain: &str, pw_encrypt: &str) -> Result<bool> {
    if pw_encrypt.len() < PWD_LEN || !pw_encrypt.starts_with(SALT_MAGIC) {
        anyhow::bail!("密码格式错误")
    }

    let digest = pw_encrypt.as_bytes();
    let salt_base64 = &digest[SALT_MAGIC.len()..DIGEST_OFFSET - 1];

    let mut pass_out = [0; PWD_LEN];
    do_encrypt(&mut pass_out, pw_plain.as_bytes(), salt_base64);

    let verify_result = pass_out == digest;
    if !verify_result {
        log::trace!("密码校验错误: 原密码 = [{}], 计算结果 = [{}], 期望结果 = [{}]",
                pw_plain, std::str::from_utf8(&pass_out).unwrap(), pw_encrypt);
    }

    Ok(verify_result)
}

/// 生成随机密码
pub fn rand_password(len: usize) -> String {
    const CHS: [&[u8]; 4] = [
        b"abcdefghijklmnopqrstuvwxyz",
        b"ABDEFGHJLMNQRTY",
        b"23456789",
        b"!@#$%^&-+",
    ];

    assert!(len >= CHS.len());

    let mut pwd = vec![b'*'; len];
    let mut rng = rand::thread_rng();

    for item in CHS.iter().skip(1) {
        let item_pos = rng.gen_range(0..item.len());
        let pwd_pos = loop {
            let p = rng.gen_range(0..pwd.len());
            if pwd[p] == b'*' {
                break p;
            }
        };
        pwd[pwd_pos] = item[item_pos];
    }

    for c in pwd.iter_mut() {
        if *c == b'*' {
            *c = CHS[0][rng.gen_range(0..CHS[0].len())];
        }
    }

    unsafe { String::from_utf8_unchecked(pwd) }
}

fn gensalt(out: &mut [u8]) {
    debug_assert!(out.len() == SALT_LEN);
    let mut rng = rand::thread_rng();
    for item in out.iter_mut().take(SALT_LEN) {
        *item = CRYPT_B64_CHARS[rng.gen_range(0..CRYPT_B64_CHARS.len())];
    }
}

fn do_encrypt(out: &mut [u8], password: &[u8], salt: &[u8]) {
    // 加密方式 Uinx Md5Crypt
    debug_assert!(out.len() >= PWD_LEN && salt.len() == SALT_LEN);

    // 计算 salt_prefix + salt + password 的 md5
    let mut hasher = Md5::new();
    hasher.update(SALT_MAGIC.as_bytes());
    hasher.update(salt);
    hasher.update(password);
    let final_state = hasher.finalize();

    // 将 "$1$" 写入返回参数
    let fs = &mut out[..SALT_MAGIC.len()];
    fs.copy_from_slice(SALT_MAGIC.as_bytes());

    // 将 salt 内容写入返回参数
    let fs = &mut out[SALT_MAGIC.len()..DIGEST_OFFSET - 1];
    fs.copy_from_slice(salt);

    // 将 "$" 写入返回参数
    out[DIGEST_OFFSET - 1] = b'$';

    // 将 password 加密后的结果进行base64编码，并写入返回参数
    let fs = &mut out[DIGEST_OFFSET..];
    for i in 0..5 {
        let (i3, i4) = (i * 3, i * 4);
        u8_to_b64(&mut fs[i4..i4+4], final_state[i3], final_state[i3+1],  final_state[i3+2]);
    }
    u8_to_b64_1(&mut fs[20..22], final_state[15]);
}

fn u8_to_b64(out: &mut [u8], b1: u8, b2: u8, b3: u8) {
    out[0] = CRYPT_B64_CHARS[(b1 >> 2) as usize];
    out[1] = CRYPT_B64_CHARS[(((b1 << 4) & 0x3f) | (b2 >> 4)) as usize];
    out[2] = CRYPT_B64_CHARS[(((b2 << 2) & 0x3f) | (b3 >> 6)) as usize];
    out[3] = CRYPT_B64_CHARS[(b3 & 0x3f) as usize];
}

fn u8_to_b64_1(out: &mut [u8], b1: u8) {
    out[0] = CRYPT_B64_CHARS[(b1 >> 2) as usize];
    out[1] = CRYPT_B64_CHARS[((b1 << 4) & 0x3f) as usize];
}

#[cfg(test)]
mod tests {
    use super::{rand_password, encrypt, verify};

    #[test]
    fn test_rand_password() {
        for _ in 0..10 {
            println!("{}", rand_password(8))
        }
    }

    #[test]
    fn test_encrypt() {
        println!("{}", encrypt("password").unwrap());
    }

    #[test]
    fn test_verify() {
        const ENC: &str = "$74$AtXyaPfN$72lU8lC7chwBrLucD4ZYD.";
        assert!(verify("password", ENC).unwrap());
    }
}
