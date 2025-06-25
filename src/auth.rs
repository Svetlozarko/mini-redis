use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub password: Option<String>,
}

impl AuthConfig {
    pub fn new(password: Option<String>) -> Self {
        Self { password }
    }

    pub fn is_auth_required(&self) -> bool {
        self.password.is_some()
    }

    pub fn verify_password(&self, provided_password: &str) -> bool {
        match &self.password {
            Some(password) => password == provided_password,
            None => true, // No password required
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientAuth {
    pub is_authenticated: bool,
    pub auth_config: Arc<AuthConfig>,
}

impl ClientAuth {
    pub fn new(auth_config: Arc<AuthConfig>) -> Self {
        Self {
            is_authenticated: !auth_config.is_auth_required(),
            auth_config,
        }
    }

    pub fn authenticate(&mut self, password: &str) -> bool {
        if self.auth_config.verify_password(password) {
            self.is_authenticated = true;
            true
        } else {
            false
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.is_authenticated
    }

    pub fn requires_auth(&self) -> bool {
        self.auth_config.is_auth_required() && !self.is_authenticated
    }
}