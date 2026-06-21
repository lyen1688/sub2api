-- Add optional User-Agent metadata to reusable TLS fingerprint profiles.
-- This lets OpenAI TLS profile binding apply the captured HTTP User-Agent
-- together with the replayed ClientHello.
ALTER TABLE tls_fingerprint_profiles
    ADD COLUMN IF NOT EXISTS user_agent VARCHAR(512) NOT NULL DEFAULT '';

COMMENT ON COLUMN tls_fingerprint_profiles.user_agent IS 'Optional upstream HTTP User-Agent paired with this TLS fingerprint profile';
