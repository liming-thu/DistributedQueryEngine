$(document).ready(function () {
    function makeElement(node) {
        var label;
        if (node.OpType === "LEAF") {
            label = node.TabName;
            if (node.Site > 0) {
                 label = label + '_' + node.Site;
            }
        } else {
            var regex = /,/g;
            var condition = node.Condition.replace(regex, "\n");
            label = node.OpType + '\n' + condition;
        }
        var letterSize = 8;
        var maxLineLength = _.max(label.split('\n'), function (l) { return l.length; }).length;
        var width = 2 * (letterSize * (0.4 * maxLineLength + 1));
        var height = 2 * ((label.split('\n').length + 1) * letterSize);

        var color;
        switch (node.Site) {
            case '1': color = '#5ff'; break;
            case '2': color = '#f5f'; break;
            case '3': color = '#ff5'; break;
            case '4': color = '#55f'; break;
            default:
                color = '#fff';
                break;
        }
        return new joint.shapes.basic.Rect({
            id: node.NodeGuid,
            size: { width: width, height: height },
            attrs: {
                text: { text: label, 'font-size': letterSize, 'font-family': 'monospace' },
                rect: {
                    width: width, height: height,
                    rx: 5, ry: 5,
                    stroke: '#555',
                    fill: color
                }
            }
        });
    }

    function makeLink(nodeA, nodeB) {
        return new joint.dia.Link({
            source: { id: nodeA.NodeGuid },
            target: { id: nodeB.NodeGuid },
            attrs: { '.marker-target': { d: 'M 4 0 L 0 2 L 4 4 z' } },
            smooth: true
        });
    }

    function buildElements(node) {
        node.visited = true;
        var elements = [];
        elements.push(makeElement(node));
        if (node.OpType !== "LEAF" && node.Oprands) {
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
                if (oprand.OpType !== "LEAF") {
                    links = links.concat(buildLinks(oprand));
                }
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
            width: 2000,
            height:1000,
            model: graph,
            gridSize: 1
        });

        var cells = buildTree(tree);
        graph.resetCells(cells);
        joint.layout.DirectedGraph.layout(graph, { setLinkVertices: false });
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
                var tree = JSON.parse(data);
                $("p#alg-tree").text();
                drawTree('alg-tree', tree.original);
                drawTree('optimized-tree', tree.optimized);
                $("textarea#sql").text(tree.statistics);
            },
            error: function(req, error) {
                console.log(req);
            }
        });
    });
});