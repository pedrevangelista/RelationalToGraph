from utils.Geral import medir_tempo


@medir_tempo
def gerar_vertice_janus(no_generico):
    vertices = []

    # Processar os vértices
    for no in no_generico:
        label = no["type"]
        props = no["properties"]
        vertices.append({
            "label": label,
            "properties": props
        })

    return vertices

@medir_tempo
def gerar_edges_janus(arestas_generica):
    edges = []

    for aresta in arestas_generica:
        props = aresta["properties"]

        # Pegar os valores de source e target a partir das chaves definidas
        source_value = aresta["from"]
        target_value = aresta["to"]

        edges.append({
            "from": source_value,
            "to": target_value,
            "label": aresta["type"],
            "properties": props
        })
    return edges