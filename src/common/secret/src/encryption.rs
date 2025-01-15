// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use aws_lc_rs::aead::{Aad, Nonce, RandomizedNonceKey, AES_128_GCM};
use serde::{Deserialize, Serialize};

use super::{SecretError, SecretResult};

#[derive(Deserialize, Serialize)]
pub struct SecretEncryption {
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

impl SecretEncryption {
    pub fn encrypt(key: &[u8], plaintext: &[u8]) -> SecretResult<Self> {
        let encrypt_key = Self::fill_key(key);
        let nonce_key = RandomizedNonceKey::new(&AES_128_GCM, &encrypt_key)
            .map_err(|_| SecretError::AesError)?;
        let mut in_out = plaintext.to_vec();
        let nonce = nonce_key
            .seal_in_place_append_tag(Aad::empty(), &mut in_out)
            .map_err(|_| SecretError::AesError)?;
        Ok(Self {
            nonce: *nonce.as_ref(),
            ciphertext: in_out,
        })
    }

    pub fn decrypt(&self, key: &[u8]) -> SecretResult<Vec<u8>> {
        let decrypt_key = Self::fill_key(key);
        let nonce_key = RandomizedNonceKey::new(&AES_128_GCM, &decrypt_key)
            .map_err(|_| SecretError::AesError)?;
        let mut in_out = self.ciphertext.clone();
        let nonce = Nonce::assume_unique_for_key(self.nonce);
        let plaintext = nonce_key
            .open_in_place(nonce, Aad::empty(), &mut in_out)
            .map_err(|_| SecretError::AesError)?;
        Ok(plaintext.to_vec())
    }

    fn fill_key(key: &[u8]) -> Vec<u8> {
        let mut k = key[..(std::cmp::min(key.len(), 16))].to_vec();
        k.resize_with(16, || 0);
        k
    }

    pub fn serialize(&self) -> SecretResult<Vec<u8>> {
        let res = bincode::serialize(&self)?;
        Ok(res)
    }

    pub fn deserialize(data: &[u8]) -> SecretResult<Self> {
        let res = bincode::deserialize(data)?;
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_secret_encryption_decyption() {
        let key = &hex::decode("0123456789abcdef0123456789abcdef").unwrap();
        assert!(key.len() == 16);
        let plaintext = "Hello, world!".as_bytes();
        let secret = SecretEncryption::encrypt(key, plaintext).unwrap();
        let decrypted = secret.decrypt(key).unwrap();
        assert_eq!(plaintext, decrypted.as_slice());
    }
}
