import pytest
from chat_testkit import MemDB


@pytest.fixture
def memdb():
    return MemDB(graph=False)


@pytest.fixture
def graphdb():
    return MemDB(graph=True)
