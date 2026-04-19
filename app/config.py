from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_password: str = Field(default="", alias="REDIS_PASSWORD")
    frontend_origin: str = Field(default="http://localhost:3000", alias="FRONTEND_ORIGIN")
    frontend_origins: str = Field(default="", alias="FRONTEND_ORIGINS")
    use_proxy: bool = Field(default=False, alias="USE_PROXY")
    proxy_ips: str = Field(default="", alias="PROXY_IPS")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def proxy_list(self) -> list[str]:
        if not self.proxy_ips:
            return []
        return [p.strip() for p in self.proxy_ips.split(",") if p.strip()]

    @property
    def allowed_frontend_origins(self) -> list[str]:
        origins = []
        if self.frontend_origin:
            origins.append(self.frontend_origin.strip())

        if self.frontend_origins:
            for origin in self.frontend_origins.split(","):
                cleaned = origin.strip()
                if cleaned:
                    origins.append(cleaned)

        # Always keep localhost for local development convenience.
        origins.append("http://localhost:3000")

        # Deduplicate while preserving order.
        return list(dict.fromkeys(origins))


settings = Settings()
