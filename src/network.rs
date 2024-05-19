use anyhow::Result;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::info;

use crate::{
    cmd::{Command, CommandExecutor as _},
    Backend, RespDecoder as _, RespEncoder, RespError, RespFrame,
};

#[derive(Debug)]
struct RespFrameCodec;

pub async fn process_stream(stream: TcpStream, backend: Backend) -> Result<()> {
    let mut frames = Framed::new(stream, RespFrameCodec);
    loop {
        match frames.next().await {
            Some(Ok(frame)) => {
                info!("Received frame: {:?}", frame);
                let frame = frame_handler(frame, &backend).await?;
                info!("Sending frame: {:?}", frame);
                frames.send(frame).await?;
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(()),
        }
    }
}

async fn frame_handler(frame: RespFrame, backend: &Backend) -> Result<RespFrame> {
    let cmd = Command::try_from(frame)?;
    info!("Executing command: {:?}", cmd);
    let frame = cmd.execute(backend);
    Ok(frame)
}

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut bytes::BytesMut) -> Result<()> {
        let data = item.encode();
        dst.extend_from_slice(&data);
        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>> {
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespError::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
