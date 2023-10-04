use tokio_native_tls::{
    native_tls::{self, Identity},
    TlsAcceptor, TlsConnector,
};

pub const SERVER_NAME: &str = "noir";

pub fn client_config() -> TlsConnector {
    let config = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .unwrap();
    TlsConnector::from(config)
}

// pub async fn connect_tls_stream(addr: &str, tls_connector: &TlsConnector) -> io::Result<TlsStream<TcpStream>> {
//     let tcp_stream = TcpStream::connect(addr).await?;
//     let tls_stream = tls_connector.connect(SERVER_NAME, tcp_stream).await.unwrap();
//     Ok(tls_stream)
// }

pub fn server_config() -> TlsAcceptor {
    let identity = load_self_signed_cert().expect("certificate");
    let config = native_tls::TlsAcceptor::builder(identity).build().unwrap();
    TlsAcceptor::from(config)
}

// fn generate_self_signed_cert() -> Result<Identity, Box<dyn std::error::Error>> {
//     let cert = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])?;
//     let key = cert.serialize_private_key_der();
//     let cert = cert.serialize_der().unwrap();
//     let identity = native_tls::Identity::from_pkcs8(&cert, &key).unwrap();
//     Ok(identity)
// }

fn load_self_signed_cert() -> Result<Identity, Box<dyn std::error::Error>> {
    let identity = native_tls::Identity::from_pkcs8(
        include_bytes!("./selfsign.crt").as_ref(),
        include_bytes!("./selfsign.key").as_ref(),
    )
    .unwrap();

    // let cert = rustls::Certificate(
    //     rustls_pemfile::certs(&mut BufReader::new(
    //         include_bytes!("./selfsign.crt").as_ref(),
    //     ))
    //     .unwrap()
    //     .pop()
    //     .unwrap(),
    // );
    // let key = rustls::PrivateKey(
    //     rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(
    //         include_bytes!("./selfsign.key").as_ref(),
    //     ))
    //     .unwrap()
    //     .pop()
    //     .unwrap(),
    // );
    Ok(identity)
}
