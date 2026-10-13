//! Normalize validated HTTP/2 pseudo-headers without accepting HTTP/1 proxy-form destinations.
pub fn normalize_h2<B>(request: &mut http::Request<B>) -> Result<(), &'static str> {
    if request.version() != http::Version::HTTP_2 {
        return Ok(());
    }
    let Some(authority) = request.uri().authority() else {
        return if request.uri().scheme().is_some() {
            Err("missing authority")
        } else {
            Ok(())
        };
    };
    if !matches!(request.uri().scheme_str(), Some("http" | "https"))
        || authority.as_str().len() > 256
        || authority.as_str().contains('@')
    {
        return Err("invalid HTTP/2 authority");
    }
    let authority = authority.as_str().to_owned();
    let mut hosts = request.headers().get_all("host").iter();
    if hosts.next().is_some_and(|h| {
        h.to_str()
            .map_or(true, |h| !h.eq_ignore_ascii_case(&authority))
    }) || hosts.next().is_some()
    {
        return Err("conflicting HTTP/2 authority");
    }
    let host = authority.parse().map_err(|_| "invalid authority header")?;
    let path = request
        .uri()
        .path_and_query()
        .ok_or("missing path")?
        .clone();
    let mut parts = http::uri::Parts::default();
    parts.path_and_query = Some(path);
    *request.uri_mut() = http::Uri::from_parts(parts).map_err(|_| "invalid path")?;
    request.headers_mut().insert("host", host);
    Ok(())
}
