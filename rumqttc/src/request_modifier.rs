//! Request modifier types for websocket connections.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Type alias for the request modifier function stored in MqttOptions.
pub(crate) type RequestModifierFn = Arc<
    dyn Fn(
            http::Request<()>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            http::Request<()>,
                            Box<dyn std::error::Error + Send + Sync>,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

/// Trait to convert request modifier output to Result, enabling backwards compatibility.
/// Accepts both `http::Request<()>` (infallible) and `Result<http::Request<()>, E>` (fallible).
pub trait IntoModifierResult {
    type Error: std::error::Error + Send + Sync + 'static;
    fn into_modifier_result(self) -> Result<http::Request<()>, Self::Error>;
}

impl IntoModifierResult for http::Request<()> {
    type Error = std::convert::Infallible;
    fn into_modifier_result(self) -> Result<http::Request<()>, Self::Error> {
        Ok(self)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> IntoModifierResult
    for Result<http::Request<()>, E>
{
    type Error = E;
    fn into_modifier_result(self) -> Result<http::Request<()>, Self::Error> {
        self
    }
}
