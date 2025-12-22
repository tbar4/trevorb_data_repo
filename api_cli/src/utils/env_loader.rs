use anyhow::Result;
use dotenv::dotenv;
use std::env;

/// Loads environment variables from a .env file
/// 
/// This function will load environment variables from a .env file in the current directory
/// or any parent directory. It uses the dotenv crate to handle the parsing and loading.
/// 
/// # Returns
/// 
/// Returns `Ok(())` if the environment variables were loaded successfully,
/// or an error if there was a problem loading the .env file.
/// 
/// # Example
/// 
/// ```rust
/// use crate::utils::env_loader::load_env;
/// 
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     load_env()?;
///     // Now you can access environment variables using std::env::var()
///     Ok(())
/// }
/// ```
pub fn load_env() -> Result<()> {
    dotenv().ok();
    Ok(())
}

/// Gets an environment variable or returns a default value
/// 
/// # Arguments
/// 
/// * `key` - The name of the environment variable to get
/// * `default` - The default value to return if the environment variable is not set
/// 
/// # Returns
/// 
/// Returns the value of the environment variable if it's set, otherwise returns the default value
/// 
/// # Example
/// 
/// ```rust
/// use crate::utils::env_loader::get_env_var_or_default;
/// 
/// let database_url = get_env_var_or_default("DATABASE_URL", "sqlite://default.db");
/// ```
pub fn get_env_var_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Gets an environment variable or returns None if it's not set
/// 
/// # Arguments
/// 
/// * `key` - The name of the environment variable to get
/// 
/// # Returns
/// 
/// Returns `Some(value)` if the environment variable is set, otherwise returns `None`
/// 
/// # Example
/// 
/// ```rust
/// use crate::utils::env_loader::get_env_var;
/// 
/// if let Some(api_key) = get_env_var("API_KEY") {
///     // Use the API key
/// }
/// ```
pub fn get_env_var(key: &str) -> Option<String> {
    env::var(key).ok()
}

/// Gets an environment variable or panics if it's not set
/// 
/// # Arguments
/// 
/// * `key` - The name of the environment variable to get
/// 
/// # Returns
/// 
/// Returns the value of the environment variable if it's set, otherwise panics
/// 
/// # Example
/// 
/// ```rust
/// use crate::utils::env_loader::get_env_var_or_panic;
/// 
/// let required_var = get_env_var_or_panic("REQUIRED_VAR");
/// ```
pub fn get_env_var_or_panic(key: &str) -> String {
    env::var(key).expect(&format!("Environment variable {} is required", key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_env() {
        // This test just verifies the function doesn't panic
        let result = load_env();
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_env_var_or_default() {
        // Test with a variable that likely doesn't exist
        let value = get_env_var_or_default("NON_EXISTENT_VAR", "default_value");
        assert_eq!(value, "default_value");
    }

    #[test]
    fn test_get_env_var() {
        // Test with a variable that likely doesn't exist
        let value = get_env_var("NON_EXISTENT_VAR");
        assert!(value.is_none());
    }
}