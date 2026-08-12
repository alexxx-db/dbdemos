import dbdemos.dbdemos as dbdemos_module
from dbdemos.installer import Installer


def test_is_ai_bi_demo():
    assert Installer.is_ai_bi_demo("aibi-marketing-campaign")
    assert Installer.is_ai_bi_demo("ai-bi-marketing-campaign")
    assert not Installer.is_ai_bi_demo("lakehouse-retail-c360")


def test_install_alias_for_ai_bi_marketing_campaign(monkeypatch):
    captured = {}

    class StubInstaller:
        def __init__(self, *args, **kwargs):
            pass

        def test_premium_pricing(self):
            return True

        def install_demo(self, demo_name, *args, **kwargs):
            captured["demo_name"] = demo_name

    monkeypatch.setattr(dbdemos_module, "check_version", lambda: None)
    monkeypatch.setattr(dbdemos_module, "Installer", StubInstaller)

    dbdemos_module.install(
        "ai-bi-marketing-campaign",
        username="user@test.com",
        pat_token="test-token",
        workspace_url="https://example.cloud.databricks.com",
    )

    assert captured["demo_name"] == "aibi-marketing-campaign"
