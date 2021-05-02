const constant = (x) => () => x;

function rectCollide() {
    var nodes, sizes, masses
    var size = constant([0, 0])
    var strength = 1
    var iterations = 1

    function force() {
        var node, size, mass, xi, yi
        var i = -1
        while (++i < iterations) { iterate() }

        function iterate() {
            var j = -1
            var tree = d3.quadtree(nodes, xCenter, yCenter).visitAfter(prepare)

            while (++j < nodes.length) {
                node = nodes[j]
                size = sizes[j]
                mass = masses[j]
                xi = xCenter(node)
                yi = yCenter(node)

                tree.visit(apply)
            }
        }

        function apply(quad, x0, y0, x1, y1) {
            var data = quad.data
            var xSize = (size[0] + quad.size[0]) / 2
            var ySize = (size[1] + quad.size[1]) / 2
            if (data) {
                if (data.index <= node.index) { return }

                var x = xi - xCenter(data)
                var y = yi - yCenter(data)
                var xd = Math.abs(x) - xSize
                var yd = Math.abs(y) - ySize

                if (xd < 0 && yd < 0) {
                    var l = Math.sqrt(x * x + y * y)
                    var m = masses[data.index] / (mass + masses[data.index])

                    if (Math.abs(xd) < Math.abs(yd)) {
                        node.vx -= (x *= xd / l * strength) * m
                        data.vx += x * (1 - m)
                    } else {
                        node.vy -= (y *= yd / l * strength) * m
                        data.vy += y * (1 - m)
                    }
                }
            }

            return x0 > xi + xSize || y0 > yi + ySize ||
                x1 < xi - xSize || y1 < yi - ySize
        }

        function prepare(quad) {
            if (quad.data) {
                quad.size = sizes[quad.data.index]
            } else {
                quad.size = [0, 0]
                var i = -1
                while (++i < 4) {
                    if (quad[i] && quad[i].size) {
                        quad.size[0] = Math.max(quad.size[0], quad[i].size[0])
                        quad.size[1] = Math.max(quad.size[1], quad[i].size[1])
                    }
                }
            }
        }
    }

    function xCenter(d) { return d.x + d.vx + sizes[d.index][0] / 2 }
    function yCenter(d) { return d.y + d.vy + sizes[d.index][1] / 2 }

    force.initialize = function (_) {
        sizes = (nodes = _).map(size)
        masses = sizes.map(function (d) { return d[0] * d[1] })
    }

    force.size = function (_) {
        return (arguments.length
            ? (size = typeof _ === 'function' ? _ : constant(_), force)
            : size)
    }

    force.strength = function (_) {
        return (arguments.length ? (strength = +_, force) : strength)
    }

    force.iterations = function (_) {
        return (arguments.length ? (iterations = +_, force) : iterations)
    }

    return force
}

let gravityEnabled = true;
let gravityCallbacks = [];

const changeGravity = (enabled) => {
    gravityEnabled = enabled;
    for (const callback of gravityCallbacks) callback();
};

const resetNetwork = () => {
    gravityCallbacks = [];
};

const drawNetwork = (nodes, links) => {
    const nodeById = {};

    const root = {
        id: -1,
        data: {},
        children: nodes,
        parent: null,
    };

    const dfs = (node, depth) => {
        nodeById[node.id] = node;
        node.depth = depth;
        if (node.children) {
            node.links = [];
            for (const child of node.children) {
                child.parent = node;
                dfs(child, depth + 1);
            }
        }
    };

    dfs(root, 0);

    // put each link in the lowest-common-ancestor of the 2 nodes
    const groupLinks = (links) => {
        for (const link of links) {
            let source = nodeById[link.source];
            let target = nodeById[link.target];

            while (source.depth > target.depth) source = source.parent;
            while (target.depth > source.depth) target = target.parent;

            // link cannot point to a children
            if (source.id === target.id) {
                throw new Error("Invalid link from " + link.source + " to " + link.target);
            }

            while (source.parent.id !== target.parent.id) {
                source = source.parent;
                target = target.parent;
            }

            const parent = source.parent;
            parent.links.push({
                source: source.id,
                target: target.id,
                link: link,
            });
        }
    };

    groupLinks(links);

    const assignInitialPositions = (node) => {
        if (!node.children) return;
        const adj = {};
        for (const link of node.links) {
            if (!(link.source in adj)) adj[link.source] = [];
            adj[link.source].push(link.target);
        }
        const visited = {};
        const order = [];
        const dfs = (node) => {
            if (node in visited) return;
            visited[node] = true;
            if (node in adj) {
                for (const next of adj[node]) {
                    dfs(next);
                }
            }
            order.push(node);
        }
        const ranks = {};
        for (const child of node.children) {
            dfs(child.id);
        }
        for (const child of node.children) {
            visited[child.id] = false;
            ranks[child.id] = 0;
        }
        for (let i = order.length - 1; i >= 0; i--) {
            const id = order[i];
            if (id in adj) {
                for (const next of adj[id]) {
                    if (visited[next]) continue;
                    ranks[next] = Math.max(ranks[next], ranks[id] + 1);
                }
            }
            visited[id] = true;
        }

        const rankFreq = {};
        const rankSpacingX = 400;
        const rankSpacingY = 200;
        const initialPosition = {};
        for (let i = order.length - 1; i >= 0; i--) {
            const id = order[i];
            const rank = ranks[id];
            const y = rankSpacingY * rank;
            if (!(rank in rankFreq)) rankFreq[rank] = 0;
            const x = rankSpacingX * rankFreq[rank];
            rankFreq[rank]++;
            initialPosition[id] = [x, y];
        }

        for (const child of node.children) {
            const [x, y] = initialPosition[child.id];
            child.x = x;
            child.y = y;

            assignInitialPositions(child);
        }
    };

    assignInitialPositions(root);

    const contentId = "network-content";

    const container = document.getElementById(contentId);
    const svgWidth = container.clientWidth;
    const svgHeight = container.clientHeight;
    const nodeWidth = 100;
    const nodeHeight = 40;

    d3.select("#" + contentId).select("svg").remove();
    const svg = d3.select("#" + contentId)
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);

    const defs = svg.append("defs");
    defs
        .append("marker")
        .attr("id", "arrowhead")
        .attr("markerWidth", "10")
        .attr("markerHeight", "7")
        .attr("refX", "7.14")
        .attr("refY", "3.5")
        .attr("orient", "auto")
        .append("polygon")
        .attr("points", "0 0, 10 3.5, 0 7");

    const nodeSize = (node) => {
        if (!node.children) return [
            node.x, node.y, node.width, node.height,
        ];

        const padding = 10;
        let left = 1e9;
        let right = -1e9;
        let top = 1e9;
        let bottom = -1e9;
        for (const child of node.children) {
            const childLeft = child.x - child.width / 2;
            const childRight = childLeft + child.width;
            const childTop = child.y - child.height / 2;
            const childBottom = childTop + child.height;

            if (childLeft < left) left = childLeft;
            if (childRight > right) right = childRight;
            if (childTop < top) top = childTop;
            if (childBottom > bottom) bottom = childBottom;
        }
        const width = right - left + 2 * padding;
        const height = bottom - top + 2 * padding;
        return [left - padding, top - padding, width, height];
    };

    function pointOnRect(x, y, minX, minY, maxX, maxY) {
        const midX = (minX + maxX) / 2;
        const midY = (minY + maxY) / 2;
        // if (midX - x == 0) -> m == ±Inf -> minYx/maxYx == x (because value / ±Inf = ±0)
        const m = (midY - y) / (midX - x);

        if (x <= midX) { // check "left" side
            const minXy = m * (minX - x) + y;
            if (minY <= minXy && minXy <= maxY)
                return [minX, minXy];
        }

        if (x >= midX) { // check "right" side
            const maxXy = m * (maxX - x) + y;
            if (minY <= maxXy && maxXy <= maxY)
                return [maxX, maxXy];
        }

        if (y <= midY) { // check "top" side
            const minYx = (minY - y) / m + x;
            if (minX <= minYx && minYx <= maxX)
                return [minYx, minY];
        }

        if (y >= midY) { // check "bottom" side
            const maxYx = (maxY - y) / m + x;
            if (minX <= maxYx && maxYx <= maxX)
                return [maxYx, maxY];
        }

        // edge case when finding midpoint intersection: m = 0/0 = NaN
        if (x === midX && y === midY) return [x, y];
    }

    const drawNode = (node, parent) => {
        if (!node.children) {
            node.width = nodeWidth;
            node.height = nodeHeight;
            const group = parent.append("g");
            group
                .append("rect")
                .style("fill", "#dddddd")
                .style("stroke", "blue")
                .attr("x", -nodeWidth/2)
                .attr("y", -nodeHeight/2)
                .attr("width", nodeWidth)
                .attr("height", nodeHeight);
            if (node.data && node.data.text) {
                group
                    .append("text")
                    .attr("text-anchor", "middle")
                    .attr("dominant-baseline", "middle")
                    .attr("font-size", "22")
                    .text(node.data.text)
            }
            if (node.data && node.data.onclick) {
                group.on("click", () => node.data.onclick());
                group.style("cursor", "pointer");
            }
            return;
        }

        const innerRect = parent
            .append("rect")
            .attr("class", "node" + node.id)
            .style("fill", "white")
            .style("stroke", "blue");

        const innerNodes = parent
            .selectAll("g")
            .data(node.children, (d) => d.id)
            .enter()
            .append("g")
            .attr("id", (d) => "node" + d.id);

        const innerText = parent
            .append("text")
            .attr("text-anchor", "start")
            .attr("dominant-baseline", "middle")
            .attr("font-size", "22")
            .text(node.data.text);

        innerNodes.each((inner, i, innerNodeElements) => {
            const innerNodeElement = d3.select(innerNodeElements[i]);
            drawNode(node.children[i], innerNodeElement);
        });

        const links = node.links.map((link) => {
            const {type, text, width, onclick} = link.link.data;
            const lineElem = parent
                .append("line")
                .attr("class", link.link.source + "_" + link.link.target)
                .attr("stroke", "black")
                .attr("stroke-width", width || 1)
                .attr("marker-end", "url(#arrowhead)");
            const textElem = parent
                .append("text")
                .attr("text-anchor", "start")
                .attr("dominant-baseline", "middle")
                .attr("font-size", "22")
                .text(text || "");
            if (type === "dashed") {
                lineElem.attr("stroke-dasharray", "4")
            }
            if (onclick) {
                lineElem
                    .style("cursor", "pointer")
                    .on("click", () => onclick());
                textElem
                    .style("cursor", "pointer")
                    .on("click", () => onclick());
            }
            return [lineElem, textElem];
        });

        const tick = () => {
            innerNodes
                .attr("transform", (d) => "translate(" + d.x + "," + d.y + ")");

            if (node.parent) {
                const [x, y, width, height] = nodeSize(node);
                node.width = width;
                node.height = height;
                innerRect
                    .attr("x", x)
                    .attr("y", y)
                    .attr("width", width)
                    .attr("height", height);
                innerText
                    .attr("x", x+20)
                    .attr("y", y-20)
                    .attr("width", width)
                    .attr("height", height);
            }

            links.forEach(([line, text], i) => {
                const link = node.links[i];
                const getCoord = (child) => {
                    if (!child || child.id === node.id) {
                        return [0, 0];
                    }
                    const [x, y] = getCoord(child.parent);
                    return [x+child.x, y+child.y];
                };
                const source = nodeById[link.link.source];
                const target = nodeById[link.link.target];
                const [x1, y1] = getCoord(source);
                const [x2, y2] = getCoord(target);

                const rect1 = [x1-source.width/2, y1-source.height/2, x1+source.width/2, y1+source.height/2];
                const rect2 = [x2-target.width/2, y2-target.height/2, x2+target.width/2, y2+target.height/2];
                const [px1, py1] = pointOnRect(x2, y2, ...rect1);
                const [px2, py2] = pointOnRect(x1, y1, ...rect2);

                line
                    .attr("x1", px1)
                    .attr("y1", py1)
                    .attr("x2", px2)
                    .attr("y2", py2);
                text
                    .attr("x", (px1 + px2) / 2)
                    .attr("y", (py1 + py2) / 2);
            });
        };

        const collisionForce = rectCollide()
            .size((d) => {
                const [,, w, h] = nodeSize(d);
                return [w, h];
            });
        const simulation = d3.forceSimulation()
            .force("link", d3.forceLink().id((d) => d.id).strength(0.001))
            .force("charge", d3.forceManyBody().strength(-30))
            .force("collision", collisionForce)
            .force("center", d3.forceCenter(0, 0))
            .alphaMin(0.1);

        simulation.nodes(node.children).on("tick", tick);
        simulation.force("link").links(node.links);
        simulation.alpha(1).restart();

        gravityCallbacks.push(() => {
            if (gravityEnabled) {
                for (const n of node.children) {
                    node.fx = null;
                    node.fy = null;
                    simulation.alpha(1).restart();
                }
            } else {
                for (const n of node.children) {
                    node.fx = node.x;
                    node.fy = node.y;
                }
            }
        });

        const drag = () => {
            return d3.drag()
                .on("start", (d) => {
                    if (!d3.event.active) simulation.alpha(1).restart();
                    d.fx = d.x;
                    d.fy = d.y;
                })
                .on("drag", (d) => {
                    d.fx = d3.event.x;
                    d.fy = d3.event.y;
                })
                .on("end", (d) => {
                    if (!d3.event.active) simulation.alphaTarget(0);
                    if (gravityEnabled) {
                        d.fx = null;
                        d.fy = null;
                    }
                });
        }

        innerNodes.call(drag());
    };

    const rootElem = svg.append("g");
    const contentElem = rootElem
        .append("g")
        .attr("transform", "translate(" + svgWidth/2 + "," + svgHeight/2 + ")");
    drawNode(root, contentElem);

    const zoom = d3.zoom()
        .scaleExtent([0.1, 10])
        .on("zoom", () => rootElem.attr("transform", d3.event.transform));
    svg.call(zoom);

    const resize = () => {
        const width = container.clientWidth;
        const height = container.clientHeight;
        svg
            .attr("width", width)
            .attr("height", height);
    }
    window.addEventListener("resize", resize);
}