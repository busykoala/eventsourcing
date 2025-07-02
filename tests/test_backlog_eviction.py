import pytest


@pytest.mark.parametrize(
    "limit,count,expected", [(3, 5, 3), (2, 2, 2), (1, 0, 0)]
)
@pytest.mark.asyncio
async def test_backlog_eviction(pubsub, limit, count, expected, make_message):
    pubsub._backlog_limit = limit
    for i in range(count):
        await pubsub.publish("t", make_message(name=str(i), stream="t"))
    assert len(pubsub._backlog.get("t", [])) == expected
