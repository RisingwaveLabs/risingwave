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

use number_prefix::NumberPrefix;

/// convert bytes to binary pretty format
pub fn convert(num_bytes: f64) -> String {
    match NumberPrefix::binary(num_bytes) {
        NumberPrefix::Standalone(bytes) => {
            format!("{} bytes", bytes)
        }
        NumberPrefix::Prefixed(prefix, n) => {
            format!("{:.2} {}B", n, prefix)
        }
    }
}

#[cfg(test)]
mod test {
    use super::convert;

    #[test]
    fn test_bytes_convert() {
        let base = 1024_f64;

        assert_eq!(convert(1_f64), "1 bytes".to_string());
        assert_eq!(convert(base), "1.00 KiB".to_string());
        assert_eq!(convert(base * base), "1.00 MiB".to_string());
        assert_eq!(convert(base * base * base), "1.00 GiB".to_string());
    }
}
