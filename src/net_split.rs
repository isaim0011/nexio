//! Shared `split` / `into_split` halves macro for stream types.

macro_rules! define_stream_split {
    ($stream:ty) => {
        /// Borrowed read half of a stream, created by `split`.
        pub struct ReadHalf<'a> {
            stream: &'a $stream,
        }

        /// Borrowed write half of a stream, created by `split`.
        pub struct WriteHalf<'a> {
            stream: &'a $stream,
        }

        /// Owned read half of a stream, created by `into_split`.
        pub struct OwnedReadHalf {
            stream: std::rc::Rc<$stream>,
        }

        /// Owned write half of a stream, created by `into_split`.
        pub struct OwnedWriteHalf {
            stream: std::rc::Rc<$stream>,
        }

        /// Error returned when reuniting mismatched [`OwnedReadHalf`] and [`OwnedWriteHalf`].
        pub struct ReuniteError(pub OwnedReadHalf, pub OwnedWriteHalf);

        impl std::fmt::Debug for ReuniteError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("ReuniteError").finish_non_exhaustive()
            }
        }

        impl std::fmt::Display for ReuniteError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("tried to reunite halves that do not originate from the same stream")
            }
        }

        impl std::error::Error for ReuniteError {}

        impl<'a> ReadHalf<'a> {
            /// Read into `buf`.
            pub async fn read<B>(&self, buf: B) -> crate::buf::BufResult<usize, B>
            where
                B: crate::buf::IoBufMut + Send + 'static,
            {
                self.stream.read(buf).await
            }

            /// Read until `buf` is full.
            pub async fn read_exact<B>(&self, buf: B) -> crate::buf::BufResult<(), B>
            where
                B: crate::buf::IoBufMut + Send + 'static,
            {
                self.stream.read_exact(buf).await
            }
        }

        impl<'a> WriteHalf<'a> {
            /// Write from `buf`.
            pub async fn write<B>(&self, buf: B) -> crate::buf::BufResult<usize, B>
            where
                B: crate::buf::IoBuf + Send + 'static,
            {
                self.stream.write(buf).await
            }

            /// Write all initialized bytes from `buf`.
            pub async fn write_all<B>(&self, buf: B) -> crate::buf::BufResult<(), B>
            where
                B: crate::buf::IoBuf + Send + 'static,
            {
                self.stream.write_all(buf).await
            }

            /// Shut down the write half.
            pub fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
                self.stream.shutdown(how)
            }
        }

        impl OwnedReadHalf {
            /// Read into `buf`.
            pub async fn read<B>(&self, buf: B) -> crate::buf::BufResult<usize, B>
            where
                B: crate::buf::IoBufMut + Send + 'static,
            {
                self.stream.read(buf).await
            }

            /// Read until `buf` is full.
            pub async fn read_exact<B>(&self, buf: B) -> crate::buf::BufResult<(), B>
            where
                B: crate::buf::IoBufMut + Send + 'static,
            {
                self.stream.read_exact(buf).await
            }

            /// Attempt to reunite this read half with its matching write half.
            pub fn reunite(self, write: OwnedWriteHalf) -> Result<$stream, ReuniteError> {
                if std::rc::Rc::ptr_eq(&self.stream, &write.stream) {
                    drop(write);
                    Ok(std::rc::Rc::try_unwrap(self.stream)
                        .unwrap_or_else(|_| unreachable!("only two Rc handles existed")))
                } else {
                    Err(ReuniteError(self, write))
                }
            }
        }

        impl OwnedWriteHalf {
            /// Write from `buf`.
            pub async fn write<B>(&self, buf: B) -> crate::buf::BufResult<usize, B>
            where
                B: crate::buf::IoBuf + Send + 'static,
            {
                self.stream.write(buf).await
            }

            /// Write all initialized bytes from `buf`.
            pub async fn write_all<B>(&self, buf: B) -> crate::buf::BufResult<(), B>
            where
                B: crate::buf::IoBuf + Send + 'static,
            {
                self.stream.write_all(buf).await
            }

            /// Shut down the write half.
            pub fn shutdown(&self, how: std::net::Shutdown) -> std::io::Result<()> {
                self.stream.shutdown(how)
            }

            /// Attempt to reunite this write half with its matching read half.
            pub fn reunite(self, read: OwnedReadHalf) -> Result<$stream, ReuniteError> {
                read.reunite(self)
            }
        }

        impl $stream {
            /// Split this stream into borrowed read and write halves.
            pub fn split(&self) -> (ReadHalf<'_>, WriteHalf<'_>) {
                (ReadHalf { stream: self }, WriteHalf { stream: self })
            }

            /// Split this stream into owned read and write halves (`Rc`-backed, `!Send`).
            pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
                let rc = std::rc::Rc::new(self);
                (
                    OwnedReadHalf {
                        stream: std::rc::Rc::clone(&rc),
                    },
                    OwnedWriteHalf { stream: rc },
                )
            }
        }
    };
}
