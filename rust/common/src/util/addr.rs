// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;

pub fn is_local_address(server_addr: &SocketAddr, peer_addr: &SocketAddr) -> bool {
    // We only compare client address, which must be specified,
    // so there is no need to consider loopback and unspecified addresses.
    server_addr.ip() == peer_addr.ip() && server_addr.port() == peer_addr.port()
}

#[cfg(test)]
mod tests {
    use crate::util::addr::is_local_address;

    #[test]
    fn test_is_local_address() {
        let check_local = |a: &str, b: &str, result: bool| {
            assert_eq!(
                is_local_address(&a.parse().unwrap(), &b.parse().unwrap()),
                result
            );
        };
        check_local("127.0.0.1:3456", "127.0.0.1:3456", true);
        check_local("10.11.12.13:3456", "10.11.12.13:3456", true);
        check_local("10.11.12.13:3456", "0.0.0.0:3456", false);
        check_local("10.11.12.13:3456", "127.0.0.1:3456", false);
        check_local("10.11.12.13:3456", "10.11.12.13:3467", false);
    }
}
