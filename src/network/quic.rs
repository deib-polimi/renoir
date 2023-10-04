use std::{io::BufReader, sync::Arc};

use quinn::{ClientConfig, ServerConfig, TransportConfig, VarInt};

pub const SERVER_NAME: &str = "noir";

// Implementation of `ServerCertVerifier` that verifies everything as trustworthy.
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn transport_config() -> TransportConfig {
    let mut transport_config = TransportConfig::default();
    transport_config.max_concurrent_uni_streams(VarInt::from_u64(1024).unwrap());
    transport_config.initial_rtt(std::time::Duration::from_millis(50));
    transport_config.receive_window(VarInt::from_u64(10_000_000).unwrap());
    transport_config.stream_receive_window(VarInt::from_u64(5_000_000).unwrap());
    transport_config.max_idle_timeout(None);
    transport_config
}

pub fn client_config() -> ClientConfig {
    let mut crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    crypto.enable_early_data = true;


    let mut c = ClientConfig::new(Arc::new(crypto));
    c.transport_config(Arc::new(transport_config()));
    c
}

pub fn server_config() -> ServerConfig {
    let (cert, key) = generate_self_signed_cert().expect("certificate");
    let mut s = ServerConfig::with_single_cert(vec![cert], key).unwrap();
    s.transport_config(Arc::new(transport_config()));
    s
}

fn generate_self_signed_cert(
) -> Result<(rustls::Certificate, rustls::PrivateKey), Box<dyn std::error::Error>> {
    let cert = rustls::Certificate(
        rustls_pemfile::certs(&mut BufReader::new(
            include_bytes!("./selfsign.crt").as_ref(),
        ))
        .unwrap()
        .pop()
        .unwrap(),
    );
    let key = rustls::PrivateKey(
        rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(
            include_bytes!("./selfsign.key").as_ref(),
        ))
        .unwrap()
        .pop()
        .unwrap(),
    );
    // let cert = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])?;
    // let key = rustls::PrivateKey(cert.serialize_private_key_der());
    // Ok((rustls::Certificate(cert.serialize_der()?), key))
    Ok((cert, key))
}
