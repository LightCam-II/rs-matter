/*
 *
 *    Copyright (c) 2026 Project CHIP Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Client-side invoke tests exercising the `InvokeSender` API.

use rs_matter::dm::clusters::app::on_off::OnOffClient as _;
use rs_matter::error::ErrorCode;
use rs_matter::im::CmdDataTag;
use rs_matter::im::{ImClient, TxOutcome};
use rs_matter::tlv::{TLVTag, TLVWrite};
use rs_matter::utils::futures::{block_on, select, Coalesce};

use crate::common::e2e::im::echo_cluster;
use crate::common::e2e::new_default_runner;
use crate::common::init_env_logger;

#[test]
fn test_client_invoke_sender_non_chunked() {
    init_env_logger();

    let im = new_default_runner();
    im.add_default_acl();
    let handler = im.handler();

    block_on(
        select(im.run(handler), async {
            let exchange = im.initiate_exchange().await?;
            let mut sender = exchange.invoke_sender(None).await?;

            // Drive the retransmit loop until the framework hands us
            // the first response chunk.
            let mut chunk = loop {
                match sender.tx().await? {
                    TxOutcome::BuildRequest(builder) => {
                        sender = builder
                            .suppress_response(false)?
                            .timed_request(false)?
                            .invoke_requests()?
                            .push()?
                            .path(0, echo_cluster::ID, echo_cluster::Commands::EchoReq as u32)?
                            .data(|w| {
                                // EchoReq body: anonymous TLV u8 retagged at
                                // CmdDataTag::Data — same on-wire form as the
                                // existing invoke test (`[0x04, 5u8]`).
                                w.u8(&TLVTag::Context(CmdDataTag::Data as u8), 5)
                            })?
                            .end()? // close CmdData entry
                            .end()? // close InvokeRequests array
                            .end()?; // close InvokeRequestMessage → InvokeSender
                    }
                    TxOutcome::GotResponse(c) => break c,
                }
            };

            // Iterate the response-chunk loop. Non-chunked EchoReq
            // gives exactly one chunk before `complete()` returns None.
            let mut chunk_count = 0u32;
            let mut got_response = false;
            loop {
                chunk_count += 1;
                {
                    // `response()` returns `Option` — `None` for
                    // DefaultSuccess (status-only) commands; EchoReq
                    // is *not* DefaultSuccess so we expect `Some`.
                    let resp = chunk
                        .response()?
                        .expect("EchoReq has a real InvokeResponse");
                    if let Some(invoke_responses) = &resp.invoke_responses {
                        for cmd_resp in invoke_responses.iter() {
                            if cmd_resp.is_ok() {
                                got_response = true;
                            }
                        }
                    }
                }
                match chunk.complete().await? {
                    Some(next) => chunk = next,
                    None => break,
                }
            }

            assert_eq!(
                chunk_count, 1,
                "Non-chunked invoke should have exactly 1 chunk"
            );
            assert!(got_response, "Should have received an invoke response");

            Ok(())
        })
        .coalesce(),
    )
    .unwrap()
}

/// LIC-2063. A command with no response data (On) that the peer refuses: here the peer has no
/// access-list entry for us, so it answers with a status of `UnsupportedAccess`. The generated
/// client used to acknowledge that answer and return `Ok`, as for a command the peer carried
/// out; the status is now the error.
#[test]
fn test_client_no_data_command_refused_is_an_error() {
    init_env_logger();

    let im = new_default_runner();
    // no `add_default_acl()`: the peer refuses us
    let handler = im.handler();

    block_on(
        select(im.run(handler), async {
            let exchange = im.initiate_exchange().await?;
            let err = exchange
                .on_off()
                .on(1)
                .await
                .expect_err("an On the peer refused must not be Ok");
            assert_eq!(err.code(), ErrorCode::UnsupportedAccess);

            Ok(())
        })
        .coalesce(),
    )
    .unwrap()
}

/// LIC-2063. The same for a command sent to an endpoint the peer does not have: the status
/// (`UnsupportedEndpoint`) is the error.
#[test]
fn test_client_no_data_command_to_a_missing_endpoint_is_an_error() {
    init_env_logger();

    let im = new_default_runner();
    im.add_default_acl();
    let handler = im.handler();

    block_on(
        select(im.run(handler), async {
            let exchange = im.initiate_exchange().await?;
            let err = exchange
                .on_off()
                .on(99)
                .await
                .expect_err("an On to an endpoint that is not there must not be Ok");
            assert_eq!(err.code(), ErrorCode::EndpointNotFound);

            Ok(())
        })
        .coalesce(),
    )
    .unwrap()
}

/// LIC-2063, the other side: a command the peer accepts is `Ok`, as before. (The light in
/// this harness answers the command; its state machine is not run here, so the attribute is
/// not read back.)
#[test]
fn test_client_no_data_command_accepted_is_ok() {
    init_env_logger();

    let im = new_default_runner();
    im.add_default_acl();
    let handler = im.handler();

    block_on(
        select(im.run(handler), async {
            let exchange = im.initiate_exchange().await?;
            exchange.on_off().on(1).await?;

            let exchange = im.initiate_exchange().await?;
            exchange.on_off().off(1).await?;

            let exchange = im.initiate_exchange().await?;
            exchange.on_off().toggle(1).await?;

            Ok(())
        })
        .coalesce(),
    )
    .unwrap()
}
