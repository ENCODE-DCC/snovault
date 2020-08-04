def test_graph_dot(testapp):
    res = testapp.get('/profiles/graph.dot', status=200)
    assert res.content_type == 'text/vnd.graphviz'
    assert res.text


def test_graph_svg(testapp):
    res = testapp.get('/profiles/graph.svg', status=200)
    res_json = res.json
    if res_json.get('status_code') == 404:
        msg = res_json.get('message')
        assert msg == 'graph.svg is not available'
        # Force fail since graphviz is not installed on the system
        assert False
    assert res.content_type == 'image/svg+xml'
    assert res.text
