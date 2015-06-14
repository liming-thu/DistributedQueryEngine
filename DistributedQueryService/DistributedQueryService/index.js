$(document).ready(function () {
    function makeElement(node) {
        var letterSize = 8;
        var width = 2 * (letterSize * (0.6 * node.OpType.length + 1));
        var height = 2 * letterSize;

        return new joint.shapes.basic.Rect({
            id: node.GUID,
            size: { width: width, height: height },
            attrs: {
                text: { text: node.OpType, 'font-size': letterSize, 'font-family': 'monospace' },
                rect: {
                    width: width, height: height,
                    rx: 5, ry: 5,
                    stroke: '#555'
                }
            }
        });
    }

    function makeLink(nodeA, nodeB) {
        return new joint.dia.Link({
            source: { id: nodeA.GUID },
            target: { id: nodeB.GUID },
            attrs: { '.marker-target': { d: 'M 4 0 L 0 2 L 4 4 z' } },
            smooth: true
        });
    }

    function buildElements(node) {
        node.visited = true;
        var elements = [];
        elements.push(makeElement(node));
        if (node.Oprands) {
            _.each(node.Oprands, function(oprand) {
                if (!oprand.visited) {
                    elements = elements.concat(buildElements(oprand));
                }
            });
        }
        return elements;
    }

    function buildLinks(node) {
        var links = [];
        if (node.Oprands) {
            _.each(node.Oprands, function (oprand) {
                links.push(makeLink(node, oprand));
                links.concat(buildLinks(oprand));
            });
        }
        return links;
    }

    function buildTree(root) {
        var elements = buildElements(root);
        var links = buildLinks(root);
        return elements.concat(links);
    }

    function drawTree(place, tree) {
        var graph = new joint.dia.Graph;

        var paper = new joint.dia.Paper({
            el: $('p#'+place),
            width: 1000,
            height:1000,
            model: graph,
            gridSize: 1
        });

        var cells = buildTree(tree);
        graph.resetCells(cells);
    }

    $("button#summit").click(function(){
        var sql = $("textarea#sql").val();
        htmlobj=$.ajax({
            type: 'POST',
            url: "/QueryService.asmx/Sql2AlgTree",
            data: "sql=" + sql,
            dataType: 'xml',
            success: function(result) {
                var data = $(result).find("string").text();
               // var tree = JSON.parse(data);
                //drawTree('alg-tree', tree);
                $("p#alg-tree").text(data);
            },
            error: function(req, error) {
                console.log(req);
            }
        });
    });
});