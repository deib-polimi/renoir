const detailsContent = $("#details-content");

const drawJobGraph = (structures, profiler) => {
    resetGraph();
    resetNetwork();
    const [nodes, links] = buildJobGraph(structures, profiler);
    drawNetwork(nodes, links);
};

const drawExecutionGraph = (structures, profiler) => {
    resetGraph();
    resetNetwork();
    const [nodes, links] = buildExecutionGraph(structures, profiler);
    drawNetwork(nodes, links);
};

const formatBytes = (bytes) => {
    const fmt = d3.format('.1f');
    if (bytes < 1024) return `${fmt(bytes)}B`;
    if (bytes < 1024*1024) return `${fmt(bytes / 1024)}KiB`;
    if (bytes < 1024*1024*1024) return `${fmt(bytes / 1024 / 1024)}MiB`;
    return `${fmt(bytes / 1024 / 1024 / 1024)}GiB`;
};

const formatNumber = (num) => {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

const makeLink = (text, onclick) => {
    return $("<a>")
        .attr("href", "#")
        .on("click", (e) => {
            e.preventDefault();
            onclick();
        })
        .text(text);
}

const buildJobGraph = (structures, profiler) => {

    const drawOperatorDetails = (block_id, operator, replicas, linkMetrics) => {
        detailsContent.html("");
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Operator: "))
                .append($("<code>").text(operator.title))
        );
        if (operator.subtitle) {
            detailsContent.append(
                $("<p>")
                    .append($("<span>").text(operator.subtitle))
            );
        }

        const hostCounts = {};
        for (const {host_id} of replicas) {
            if (!(host_id in hostCounts)) hostCounts[host_id] = 0;
            hostCounts[host_id] += 1;
        }
        detailsContent.append($("<p>").append($("<strong>").text("Replicated at:")));
        const replicasList = $("<ul>");
        for (const [host_id, count] of Object.entries(hostCounts)) {
            replicasList.append(
                $("<li>")
                    .append($("<code>").text(`Host${host_id}`))
                    .append(` × ${count}`));
        }
        detailsContent.append(replicasList);

        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Produces: "))
                .append($("<code>").text(operator.out_type))
        );
        if (operator.connections.length > 0) {
            const list = $("<ul>");
            for (const connection of operator.connections) {
                const to_block_id = connection.to_block_id;
                const li = $("<li>")
                    .append("Block " + to_block_id + " sending ")
                    .append($("<code>").text(connection.data_type))
                    .append(" with strategy ")
                    .append($("<code>").text(connection.strategy));
                const key = ChannelMetric.blockPairKey(block_id, connection.to_block_id);
                if (key in linkMetrics.items_out) {
                    const drawMessages = () => {
                        drawProfilerGraph(linkMetrics.items_out[key].series, `Items/s in ${block_id} → ${to_block_id}`, profiler.iteration_boundaries, (v) => formatNumber(v));
                    };
                    const drawNetworkMessages = () => {
                        drawProfilerGraph(linkMetrics.net_messages_out[key].series, `Network messages/s in ${block_id} → ${to_block_id}`, profiler.iteration_boundaries);
                    };
                    const drawNetworkBytes = () => {
                        drawProfilerGraph(linkMetrics.net_bytes_out[key].series, `Network bytes/s in ${block_id} → ${to_block_id}`, profiler.iteration_boundaries, (v) => formatBytes(v)+"/s");
                    };
                    const total = linkMetrics.items_out[key].series.total;
                    li.append(": ")
                        .append(makeLink(`${formatNumber(total)} items sent`, () => drawMessages()));

                    drawMessages();
                    if (key in linkMetrics.net_messages_out) {
                        const numMex = linkMetrics.net_messages_out[key].series.total;
                        const bytes = linkMetrics.net_bytes_out[key].series.total;
                        li
                            .append(" (in ")
                            .append(makeLink(`${numMex} messages`, () => drawNetworkMessages()))
                            .append(", for a ")
                            .append(makeLink(`total of ${formatBytes(bytes)}`, () => drawNetworkBytes()))
                            .append(")");

                    }
                }
                list.append(li);
            }
            detailsContent.append($("<p>")
                .append($("<strong>").text("Connects to: "))
                .append(list));
        }
        if (operator.receivers.length > 0) {
            const list = $("<ul>");
            for (const receiver of operator.receivers) {
                const from_block_id = receiver.previous_block_id;
                const li = $("<li>")
                    .append("Block " + from_block_id + " receiving ")
                    .append($("<code>").text(receiver.data_type));
                const key = ChannelMetric.blockPairKey(receiver.previous_block_id, block_id);
                if (key in linkMetrics.items_in) {
                    const drawMessages = () => {
                        drawProfilerGraph(linkMetrics.items_in[key].series, `Items/s in ${from_block_id} → ${block_id}`, profiler.iteration_boundaries, (v) => formatNumber(v));
                    };
                    const drawNetworkMessages = () => {
                        drawProfilerGraph(linkMetrics.net_messages_in[key].series, `Network messages/s in ${from_block_id} → ${block_id}`, profiler.iteration_boundaries);
                    };
                    const drawNetworkBytes = () => {
                        drawProfilerGraph(linkMetrics.net_bytes_in[key].series, `Network bytes/s in ${from_block_id} → ${block_id}`, profiler.iteration_boundaries, (v) => formatBytes(v)+"/s");
                    };
                    const total = linkMetrics.items_in[key].series.total;
                    li.append(": ")
                        .append(makeLink(`${formatNumber(total)} items received`, () => drawMessages()));

                    drawMessages();
                    if (key in linkMetrics.net_messages_in) {
                        const numMex = linkMetrics.net_messages_in[key].series.total;
                        const bytes = linkMetrics.net_bytes_in[key].series.total;
                        li
                            .append(" (in ")
                            .append(makeLink(`${numMex} messages`, () => drawNetworkMessages()))
                            .append(", for a ")
                            .append(makeLink(`total of ${formatBytes(bytes)}`, () => drawNetworkBytes()))
                            .append(")");

                    }
                }
                list.append(li);
            }
            detailsContent.append($("<p>")
                .append($("<strong>").text("Receives data from: "))
                .append(list));
        }
    };

    const drawLinkDetails = (from_block_id, connection, linkMetrics) => {
        const to_block_id = connection.to_block_id;
        detailsContent.html("");
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Connection: "))
                .append($("<code>").text(`${from_block_id} → ${to_block_id}`))
        );
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Data type: "))
                .append($("<code>").text(connection.data_type))
        );
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Strategy: "))
                .append($("<code>").text(connection.strategy))
        );

        const metricsKey = ChannelMetric.blockPairKey(from_block_id, to_block_id);
        if (metricsKey in linkMetrics.net_messages_in) {
            const message = linkMetrics.net_messages_in[metricsKey].series.total;
            const bytes = linkMetrics.net_bytes_in[metricsKey].series.total;
            const drawNetworkMessages = () => {
                drawProfilerGraph(linkMetrics.net_messages_in[metricsKey].series, `Network messages/s in ${from_block_id} → ${to_block_id}`, profiler.iteration_boundaries);
            };
            const drawNetworkBytes = () => {
                drawProfilerGraph(linkMetrics.net_bytes_in[metricsKey].series, `Network bytes/s in ${from_block_id} → ${to_block_id}`, profiler.iteration_boundaries, (v) => formatBytes(v)+"/s");
            };
            detailsContent.append($("<p>")
                .append($("<strong>").text("Traffic: "))
                .append(makeLink(`${message} messages`, () => drawNetworkMessages()))
                .append(", for a ")
                .append(makeLink(`total of ${formatBytes(bytes)}`, () => drawNetworkBytes()))
                .append(` (${formatBytes(bytes/message)}/message)`));
            drawNetworkBytes();
        }
    }

    const linkMetrics = {
        items_in: profiler.channel_metrics.items_in.groupByBlockId(),
        items_out: profiler.channel_metrics.items_out.groupByBlockId(),
        net_messages_in: profiler.channel_metrics.net_messages_in.groupByBlockId(),
        net_messages_out: profiler.channel_metrics.net_messages_out.groupByBlockId(),
        net_bytes_in: profiler.channel_metrics.net_bytes_in.groupByBlockId(),
        net_bytes_out: profiler.channel_metrics.net_bytes_out.groupByBlockId(),
    };

    const byBlockId = {};
    const blockReplicas = {};
    for (const entry of structures) {
        const [coord, structure] = entry;
        const block_id = coord["block_id"];
        byBlockId[block_id] = structure;
        if (!(block_id in blockReplicas)) blockReplicas[block_id] = [];
        blockReplicas[block_id].push(coord);
    }
    const nodes = [];
    const links = [];
    const receivers = {};

    const operatorId = (block_id, index) => {
        return 100000 + block_id * 1000 + index;
    };

    const maxChannelBytes = Math.max(...Object.values(linkMetrics.net_bytes_in).map((d) => d.series.total));
    const linkWidth = (from_block_id, to_block_id) => {
        const key = ChannelMetric.blockPairKey(from_block_id, to_block_id);
        const minWidth = 1;
        const maxWidth = 3;
        const metric = linkMetrics.net_bytes_in[key];
        if (!metric) return minWidth;
        const value = metric.series.total;
        return minWidth + (maxWidth - minWidth) * (value / maxChannelBytes);
    }

    for (const [block_id, structure] of Object.entries(byBlockId)) {
        const block = {
            id: block_id,
            data: {
                text: "Block " + block_id
            },
            children: structure.operators.map((operator, index) => {
                return {
                    id: operatorId(block_id, index),
                    data: {
                        text: operator["title"],
                        onclick: () => drawOperatorDetails(block_id, operator, blockReplicas[block_id], linkMetrics)
                    }
                };
            })
        };
        nodes.push(block);
        structure.operators.map((operator, index) => {
            if (index < structure.operators.length - 1) {
                links.push({
                    source: operatorId(block_id, index),
                    target: operatorId(block_id, index+1),
                    data: {
                        text: operator.out_type,
                    }
                })
            }
            for (const receiver of operator.receivers) {
                const prev_block_id = receiver.previous_block_id;
                if (!(prev_block_id in receivers)) receivers[prev_block_id] = {};
                receivers[prev_block_id][block_id] = index;
            }
        });
    }
    for (const [block_id, structure] of Object.entries(byBlockId)) {
        structure.operators.map((operator, index) => {
            for (const connection of operator.connections) {
                const receiverIndex = receivers[block_id][connection.to_block_id];
                const source = operatorId(block_id, index);
                const target = operatorId(connection.to_block_id, receiverIndex);
                links.push({
                    source,
                    target,
                    data: {
                        type: "solid",
                        text: connection.data_type,
                        onclick: () => drawLinkDetails(block_id, connection, linkMetrics),
                        width: linkWidth(block_id, connection.to_block_id),
                    }
                })
            }
        });
    }

    return [nodes, links];
};

const buildExecutionGraph = (structures, profiler) => {

    const formatCoord = (coord) => {
        return `Host${coord.host_id} Block${coord.block_id} Replica${coord.replica_id}`;
    }

    const drawOperatorDetails = (coord, index, operator) => {
        detailsContent.html("");
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Operator: "))
                .append($("<code>").text(operator.title))
        );
        if (operator.subtitle) {
            detailsContent.append(
                $("<p>")
                    .append($("<span>").text(operator.subtitle))
            );
        }

        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("At: "))
                .append($("<code>").text(formatCoord(coord)))
        );

        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Produces: "))
                .append($("<code>").text(operator.out_type))
        );
        const id = operatorId(coord.host_id, coord.block_id, coord.replica_id, index);
        const conn = connections[id];
        if (conn) {
            const list = $("<ul>");
            for (const {to, connection} of conn) {
                const li = $("<li>")
                    .append($("<code>").text(formatCoord(to)))
                    .append(" sending ")
                    .append($("<code>").text(connection.data_type))
                    .append(" with strategy ")
                    .append($("<code>").text(connection.strategy));
                const key = ChannelMetric.coordPairKey(coord, to);
                if (key in profiler.channel_metrics.items_out.data) {
                    const messages = profiler.channel_metrics.items_out.data[key];
                    const net_messages = profiler.channel_metrics.net_messages_out.data[key];
                    const net_bytes = profiler.channel_metrics.net_bytes_out.data[key];
                    const drawMessages = () => {
                        drawProfilerGraph(messages.series, `Items/s in ${formatCoord(coord)} → ${formatCoord(to)}`, profiler.iteration_boundaries, (v) => formatNumber(v));
                    };
                    const drawNetworkMessages = () => {
                        drawProfilerGraph(net_messages.series, `Network messages/s in ${formatCoord(coord)} → ${formatCoord(to)}`, profiler.iteration_boundaries);
                    };
                    const drawNetworkBytes = () => {
                        drawProfilerGraph(net_bytes.series, `Network bytes/s in ${formatCoord(coord)} → ${formatCoord(to)}`, profiler.iteration_boundaries, (v) => formatBytes(v)+"/s");
                    };
                    const total = messages.series.total;
                    li.append(": ")
                        .append(makeLink(`${formatNumber(total)} items sent`, () => drawMessages()));

                    drawMessages();
                    if (net_messages) {
                        li
                            .append(" (in ")
                            .append(makeLink(`${net_messages.series.total} messages`, () => drawNetworkMessages()))
                            .append(", for a ")
                            .append(makeLink(`total of ${formatBytes(net_bytes.series.total)}`, () => drawNetworkBytes()))
                            .append(")");

                    }
                }
                list.append(li);
            }
            detailsContent.append($("<p>")
                .append($("<strong>").text("Connects to: "))
                .append(list));
        }
        const recv_conn = recv_connections[id];
        if (recv_conn) {
            const list = $("<ul>");
            for (const {from, connection} of recv_conn) {
                const li = $("<li>")
                    .append($("<code>").text(formatCoord(from)))
                    .append(" receiving ")
                    .append($("<code>").text(connection.data_type));
                const key = ChannelMetric.coordPairKey(from, coord);
                if (key in profiler.channel_metrics.items_in.data) {
                    const messages = profiler.channel_metrics.items_in.data[key];
                    const net_messages = profiler.channel_metrics.net_messages_in.data[key];
                    const net_bytes = profiler.channel_metrics.net_bytes_in.data[key];
                    const drawMessages = () => {
                        drawProfilerGraph(messages.series, `Items/s in ${formatCoord(from)} → ${formatCoord(coord)}`, profiler.iteration_boundaries, (v) => formatNumber(v));
                    };
                    const drawNetworkMessages = () => {
                        drawProfilerGraph(net_messages.series, `Network messages/s in ${formatCoord(from)} → ${formatCoord(coord)}`, profiler.iteration_boundaries);
                    };
                    const drawNetworkBytes = () => {
                        drawProfilerGraph(net_bytes.series, `Network bytes/s in ${formatCoord(from)} → ${formatCoord(coord)}`, profiler.iteration_boundaries, (v) => formatBytes(v)+"/s");
                    };
                    li.append(": ")
                        .append(makeLink(`${formatNumber(messages.series.total)} items received`, () => drawMessages()));

                    drawMessages();
                    if (net_messages) {
                        li
                            .append(" (in ")
                            .append(makeLink(`${net_messages.series.total} messages`, () => drawNetworkMessages()))
                            .append(", for a ")
                            .append(makeLink(`total of ${formatBytes(net_bytes.series.total)}`, () => drawNetworkBytes()))
                            .append(")");

                    }
                }
                list.append(li);
            }
            detailsContent.append($("<p>")
                .append($("<strong>").text("Receives data from: "))
                .append(list));
        }
    };

    const drawLinkDetails = (from, to, connection) => {
        detailsContent.html("");
        const coordPair = `${formatCoord(from)} → ${formatCoord(to)}`;
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Connection: "))
                .append($("<code>").text(coordPair))
        );
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Data type: "))
                .append($("<code>").text(connection.data_type))
        );
        detailsContent.append(
            $("<p>")
                .append($("<strong>").text("Strategy: "))
                .append($("<code>").text(connection.strategy))
        );

        const metricsKey = ChannelMetric.coordPairKey(from, to);
        if (metricsKey in profiler.channel_metrics.net_messages_in.data) {
            const messages = profiler.channel_metrics.net_messages_in.data[metricsKey].series;
            const bytes = profiler.channel_metrics.net_bytes_in.data[metricsKey].series;
            const drawNetworkMessages = () => {
                drawProfilerGraph(messages, `Network messages/s in ${coordPair}`, profiler.iteration_boundaries);
            };
            const drawNetworkBytes = () => {
                drawProfilerGraph(bytes, `Network bytes/s in ${coordPair}`, profiler.iteration_boundaries, (v) => formatBytes(v)+"/s");
            };
            detailsContent.append($("<p>")
                .append($("<strong>").text("Traffic: "))
                .append(makeLink(`${messages.total} messages`, () => drawNetworkMessages()))
                .append(", for a ")
                .append(makeLink(`total of ${formatBytes(bytes.total)}`, () => drawNetworkBytes()))
                .append(` (${formatBytes(bytes.total/messages.total)}/message)`));
            drawNetworkBytes();
        }
    }

    const byHostId = {};
    for (const entry of structures) {
        const [coord, structure] = entry;
        const host_id = coord["host_id"];
        const block_id = coord["block_id"];
        const replica_id = coord["replica_id"];
        if (!(host_id in byHostId)) byHostId[host_id] = {};
        if (!(block_id in byHostId[host_id])) byHostId[host_id][block_id] = [];
        byHostId[host_id][block_id][replica_id] = structure;
    }

    const nodes = [];
    const links = [];
    const receivers = {};
    const connections = {};
    const recv_connections = {};

    const hostId = (host_id) => {
        return `h${host_id}`;
    };
    const blockId = (host_id, block_id, replica_id) => {
        return `h${host_id}b${block_id}r${replica_id}`;
    };
    const operatorId = (host_id, block_id, replica_id, index) => {
        return `h${host_id}b${block_id}r${replica_id}o${index}`;
    };

    const maxChannelBytes = Math.max(...Object.values(profiler.channel_metrics.net_bytes_in.data).map((d) => d.series.total));
    const linkWidth = (from, to) => {
        const key = ChannelMetric.coordPairKey(from, to);
        const minWidth = 1;
        const maxWidth = 3;
        const metric = profiler.channel_metrics.net_bytes_in.data[key];
        if (!metric) return minWidth;
        const value = metric.series.total;
        return minWidth + (maxWidth - minWidth) * (value / maxChannelBytes);
    }

    const buildOperatorNode = (host_id, block_id, replica_id, operator_index, structure) => {
        for (const receiver of structure.receivers) {
            const prev_block_id = receiver.previous_block_id;
            if (!(prev_block_id in receivers)) receivers[prev_block_id] = {};
            if (!(block_id in receivers[prev_block_id])) receivers[prev_block_id][block_id] = [];
            receivers[prev_block_id][block_id].push([host_id, block_id, replica_id, operator_index]);
        }
        return {
            id: operatorId(host_id, block_id, replica_id, operator_index),
            data: {
                text: structure["title"],
                onclick: () => drawOperatorDetails({host_id, block_id, replica_id}, operator_index, structure)
            }
        }
    };

    const buildBlockNode = (host_id, block_id, structures) => {
        return structures.map((replica, replica_id) => {
            const node = {
                id: blockId(host_id, block_id, replica_id),
                data: {
                    text: `Block ${block_id} / Replica ${replica_id}`,
                },
                children: replica["operators"].map((operator, index) => buildOperatorNode(host_id, block_id, replica_id, index, operator))
            };
            // in-block connections
            replica.operators.map((operator, index) => {
                if (index < replica.operators.length - 1) {
                    links.push({
                        source: operatorId(host_id, block_id, replica_id, index),
                        target: operatorId(host_id, block_id, replica_id, index+1),
                        data: {
                            text: operator.out_type,
                        }
                    })
                }
            });
            return node;
        });
    };

    const buildHostNode = (host_id, blocks) => {
        const node = {
            id: hostId(host_id),
            data: {
                text: `Host ${host_id}`
            },
            children: Object.entries(blocks).flatMap(([block_id, structures]) => {
                return buildBlockNode(host_id, block_id, structures);
            }),
        };
        nodes.push(node);
    };

    for (const [host_id, blocks] of Object.entries(byHostId)) {
        buildHostNode(host_id, blocks);
    }

    for (const [host_id, blocks] of Object.entries(byHostId)) {
        for (const [block_id, replicas] of Object.entries(blocks)) {
            replicas.map((replica, replica_id) => {
                replica.operators.map((operator, index) => {
                    for (const connection of operator.connections) {
                        const {to_block_id, data_type, strategy} = connection;
                        const recv = receivers[block_id][to_block_id];
                        const addLink = (to) => {
                            const fromCoord = {host_id, block_id, replica_id}
                            const fromId = operatorId(host_id, block_id, replica_id, index);
                            const toCoord = {host_id: to[0], block_id: to[1], replica_id: to[2]};
                            const toId = operatorId(...to);
                            if (!(fromId in connections)) connections[fromId] = [];
                            connections[fromId].push({to: toCoord, connection});
                            if (!(toId in recv_connections)) recv_connections[toId] = [];
                            recv_connections[toId].push({from: fromCoord, connection});
                            links.push({
                                source: fromId,
                                target: toId,
                                data: {
                                    text: data_type,
                                    width: linkWidth(fromCoord, toCoord),
                                    onclick: () => drawLinkDetails(fromCoord, toCoord, connection),
                                }
                            });
                        };

                        if (strategy === "OnlyOne") {
                            if (recv.length === 1) {
                                addLink(recv[0]);
                            } else {
                                for (const r of recv) {
                                    const [host_id2, block_id2, replica_id2, operator_id2] = r;
                                    if (host_id === host_id2 && replica_id === replica_id2) {
                                        addLink(r);
                                    }
                                }
                            }
                        } else if (strategy === "GroupBy" || strategy === "Random") {
                            for (const r of recv) {
                                addLink(r);
                            }
                        } else {
                            throw Error("Invalid strategy: " + strategy);
                        }
                    }
                });
            });
        }
    }

    return [nodes, links];
};