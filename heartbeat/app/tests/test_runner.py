from heartbeat.runner import SubprocessRunner


def test_simple_command():
    r = SubprocessRunner().run(["echo", "hello"], timeout=5)
    assert r.rc == 0
    assert "hello" in r.stdout


def test_shell_pipeline():
    r = SubprocessRunner().run("echo foo | tr a-z A-Z", timeout=5, shell=True)
    assert r.rc == 0
    assert "FOO" in r.stdout


def test_nonzero_return_code():
    r = SubprocessRunner().run(["sh", "-c", "exit 3"], timeout=5)
    assert r.rc == 3


def test_timeout_yields_none_rc():
    r = SubprocessRunner().run(["sleep", "5"], timeout=0.2)
    assert r.rc is None
    assert r.stderr == "timeout"
