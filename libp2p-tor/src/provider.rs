// Copyright 2022 Hannes Furmans
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use arti_client::DataStream;
use futures::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncRead as TokioAsyncRead, AsyncWrite as TokioAsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// The underlying byte stream: either an arti `DataStream` (embedded Tor) or a plain
/// `TcpStream` already tunnelled through an external Tor daemon's SOCKS5 proxy.
/// Both impl the tokio `AsyncRead`/`AsyncWrite` traits and are `Unpin`, so we delegate
/// to whichever variant is active.
#[derive(Debug)]
enum Inner {
    Arti(DataStream),
    Socks(TcpStream),
}

#[derive(Debug)]
pub struct TokioTorStream {
    inner: Inner,
}

impl From<DataStream> for TokioTorStream {
    fn from(inner: DataStream) -> Self {
        Self {
            inner: Inner::Arti(inner),
        }
    }
}

impl From<TcpStream> for TokioTorStream {
    fn from(inner: TcpStream) -> Self {
        Self {
            inner: Inner::Socks(inner),
        }
    }
}

impl AsyncRead for TokioTorStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut read_buf = ReadBuf::new(buf);
        match &mut self.inner {
            Inner::Arti(s) => futures::ready!(TokioAsyncRead::poll_read(
                std::pin::Pin::new(s),
                cx,
                &mut read_buf
            ))?,
            Inner::Socks(s) => futures::ready!(TokioAsyncRead::poll_read(
                std::pin::Pin::new(s),
                cx,
                &mut read_buf
            ))?,
        }
        std::task::Poll::Ready(Ok(read_buf.filled().len()))
    }
}

impl AsyncWrite for TokioTorStream {
    #[inline]
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.inner {
            Inner::Arti(s) => TokioAsyncWrite::poll_write(std::pin::Pin::new(s), cx, buf),
            Inner::Socks(s) => TokioAsyncWrite::poll_write(std::pin::Pin::new(s), cx, buf),
        }
    }

    #[inline]
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.inner {
            Inner::Arti(s) => TokioAsyncWrite::poll_flush(std::pin::Pin::new(s), cx),
            Inner::Socks(s) => TokioAsyncWrite::poll_flush(std::pin::Pin::new(s), cx),
        }
    }

    #[inline]
    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.inner {
            Inner::Arti(s) => TokioAsyncWrite::poll_shutdown(std::pin::Pin::new(s), cx),
            Inner::Socks(s) => TokioAsyncWrite::poll_shutdown(std::pin::Pin::new(s), cx),
        }
    }

    #[inline]
    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.inner {
            Inner::Arti(s) => TokioAsyncWrite::poll_write_vectored(std::pin::Pin::new(s), cx, bufs),
            Inner::Socks(s) => TokioAsyncWrite::poll_write_vectored(std::pin::Pin::new(s), cx, bufs),
        }
    }
}
