//! Length-prefixed framing for IPC wire protocol.
//!
//! Uses a simple binary protocol:
//! - 4 bytes: big-endian u32 = payload length
//! - N bytes: JSON payload

use std::io;

use ghost_core::error::{GhostError, GhostResult};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Maximum allowed frame payload size (256 MB).
pub const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024;

/// Read a length-prefixed frame from an async stream.
///
/// Returns the payload bytes on success.
///
/// # Errors
///
/// Returns `GhostError::IpcError` if:
/// - The stream is closed unexpectedly
/// - The payload length exceeds `MAX_FRAME_SIZE`
/// - An I/O error occurs
pub async fn read_frame<S: AsyncReadExt + Unpin>(stream: &mut S) -> GhostResult<Vec<u8>> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            GhostError::IpcError("connection closed while reading frame length".to_string())
        } else {
            GhostError::IpcError(format!("failed to read frame length: {}", e))
        }
    })?;

    let len = u32::from_be_bytes(len_buf) as usize;

    // Guard against oversized frames
    if len > MAX_FRAME_SIZE {
        return Err(GhostError::IpcError(format!(
            "frame size {} exceeds maximum allowed size {}",
            len, MAX_FRAME_SIZE
        )));
    }

    // Read the payload
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            GhostError::IpcError("connection closed while reading frame payload".to_string())
        } else {
            GhostError::IpcError(format!("failed to read frame payload: {}", e))
        }
    })?;

    Ok(buf)
}

/// Write a length-prefixed frame to an async stream.
///
/// # Errors
///
/// Returns `GhostError::IpcError` if an I/O error occurs.
pub async fn write_frame<S: AsyncWriteExt + Unpin>(stream: &mut S, data: &[u8]) -> GhostResult<()> {
    let len = data.len() as u32;

    // Write 4-byte length prefix
    stream.write_all(&len.to_be_bytes()).await.map_err(|e| {
        GhostError::IpcError(format!("failed to write frame length: {}", e))
    })?;

    // Write the payload
    stream.write_all(data).await.map_err(|e| {
        GhostError::IpcError(format!("failed to write frame payload: {}", e))
    })?;

    // Flush to ensure data is sent
    stream.flush().await.map_err(|e| {
        GhostError::IpcError(format!("failed to flush frame: {}", e))
    })?;

    Ok(())
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{duplex, AsyncRead, AsyncWrite, ReadBuf};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A wrapper around DuplexStream that implements AsyncRead + AsyncWrite
    /// for testing frame read/write without a real Unix socket.
    struct MockStream(tokio::io::DuplexStream);

    impl AsyncRead for MockStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for MockStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }

    #[tokio::test]
    async fn test_frame_roundtrip() {
        let (a, b) = duplex(1024);

        let mut client = MockStream(a);
        let mut server = MockStream(b);

        let payload = b"test frame data";
        write_frame(&mut client, payload).await.unwrap();

        let received = read_frame(&mut server).await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn test_frame_empty_payload() {
        let (a, b) = duplex(1024);

        let mut client = MockStream(a);
        let mut server = MockStream(b);

        write_frame(&mut client, b"").await.unwrap();

        let received = read_frame(&mut server).await.unwrap();
        assert_eq!(received.len(), 0);
    }

    #[tokio::test]
    async fn test_frame_large_payload() {
        let (a, b) = duplex(65536);

        let mut client = MockStream(a);
        let mut server = MockStream(b);

        let payload = vec![0xAB; 10000];
        write_frame(&mut client, &payload).await.unwrap();

        let received = read_frame(&mut server).await.unwrap();
        assert_eq!(received, payload);
    }

    #[test]
    fn test_max_frame_size_constant() {
        assert_eq!(MAX_FRAME_SIZE, 256 * 1024 * 1024);
    }
}
