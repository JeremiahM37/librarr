def test_pipeline_raises_on_verification_failure(monkeypatch, tmp_path):
    import pipeline
    import config

    f = tmp_path / "book.epub"
    f.write_bytes(b"epub")

    class BadTarget:
        name = "fake"
        label = "Fake"

        def import_book(self, file_path, title="", author="", media_type="ebook"):
            return {"ok": True}

        def verify_import(self, file_path, title="", author="", media_type="ebook", import_result=None):
            return {"ok": False, "mode": "test", "reason": "missing"}

    monkeypatch.setattr(pipeline.targets, "get_enabled_targets", lambda: [BadTarget()])
    monkeypatch.setattr(pipeline, "organize_file", lambda file_path, *a, **k: file_path)
    monkeypatch.setattr(config, "ENABLED_TARGETS", "fake")
    monkeypatch.setattr(config, "TARGET_ROUTING_RULES", "{}")

    try:
        pipeline.run_pipeline(str(f), title="Dune", author="Frank Herbert", source="test", source_id="x")
        assert False, "expected verification failure"
    except RuntimeError as exc:
        assert "Post-import verification failed" in str(exc)


def test_pipeline_records_successful_verification(monkeypatch, tmp_path):
    import pipeline
    import config

    f = tmp_path / "book.epub"
    f.write_bytes(b"epub")

    class GoodTarget:
        name = "fake"
        label = "Fake"

        def import_book(self, file_path, title="", author="", media_type="ebook"):
            return {"id": "1"}

        def verify_import(self, file_path, title="", author="", media_type="ebook", import_result=None):
            return {"ok": True, "mode": "test"}

    monkeypatch.setattr(pipeline.targets, "get_enabled_targets", lambda: [GoodTarget()])
    monkeypatch.setattr(pipeline, "organize_file", lambda file_path, *a, **k: file_path)
    monkeypatch.setattr(config, "ENABLED_TARGETS", "fake")
    monkeypatch.setattr(config, "TARGET_ROUTING_RULES", "{}")

    result = pipeline.run_pipeline(str(f), title="Dune", author="Frank Herbert", source="test", source_id="x")
    assert result["imports"]["fake"]["id"] == "1"
    assert result["verifications"]["fake"]["ok"] is True
    assert result["verification_failed_targets"] == []


def test_pipeline_applies_target_routing_rules(monkeypatch, tmp_path):
    import pipeline
    import config

    f = tmp_path / "book.epub"
    f.write_bytes(b"epub")
    called = []

    class T1:
        name = "calibre"
        label = "Calibre"
        def import_book(self, *a, **k):
            called.append(self.name)
            return {"id": "1"}
        def verify_import(self, *a, **k):
            return {"ok": True}

    class T2:
        name = "kavita"
        label = "Kavita"
        def import_book(self, *a, **k):
            called.append(self.name)
            return {"id": "2"}
        def verify_import(self, *a, **k):
            return {"ok": True}

    monkeypatch.setattr(pipeline.targets, "get_enabled_targets", lambda: [T1(), T2()])
    monkeypatch.setattr(pipeline, "organize_file", lambda file_path, *a, **k: file_path)
    monkeypatch.setattr(config, "ENABLED_TARGETS", "calibre,kavita")
    monkeypatch.setattr(config, "TARGET_ROUTING_RULES", '{"media_type":{"ebook":["calibre"]}}')

    pipeline.run_pipeline(str(f), title="Dune", author="Frank Herbert", source="annas", source_id="x")
    assert called == ["calibre"]
