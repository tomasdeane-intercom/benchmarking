from benchkit.cli import wait_for_infra


class _StubKafkaAdmin:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers

    def cluster_ready(self, timeout: float = 5.0) -> bool:
        return True


class _StubRedis:
    def ping(self) -> None:
        return None


class _StubResponse:
    def raise_for_status(self) -> None:
        return None


def test_wait_for_infra_uses_kafka_metadata_probe(monkeypatch):
    monkeypatch.setattr(wait_for_infra, "KafkaAdmin", _StubKafkaAdmin)
    monkeypatch.setattr(wait_for_infra, "redis_client", lambda _: _StubRedis())
    monkeypatch.setattr(wait_for_infra.requests, "get", lambda *args, **kwargs: _StubResponse())
    monkeypatch.setattr(wait_for_infra.sys, "argv", ["wait_for_infra"])

    assert wait_for_infra.main() == 0
