const detailsContent = $("#details-content");

const processData = (structures, profilers) => {
    console.log(structures, profilers);
    const [nodes, links] = buildJobGraph(structures);
    drawNetwork(nodes, links);
};

const drawOperatorDetails = (operator) => {
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
            .append($("<strong>").text("Produces: "))
            .append($("<code>").text(operator.out_type))
    );
    if (operator.connections.length > 0) {
        const list = $("<ul>");
        for (const connection of operator.connections) {
            list.append(
                $("<li>")
                    .append("Block " + connection.to_block_id + " sending ")
                    .append($("<code>").text(connection.data_type))
                    .append(" with strategy ")
                    .append($("<code>").text(connection.strategy)))
        }
        detailsContent.append($("<p>")
            .append($("<strong>").text("Connects to: "))
            .append(list));
    }
    if (operator.receivers.length > 0) {
        const list = $("<ul>");
        for (const receiver of operator.receivers) {
            list.append(
                $("<li>")
                    .append("Block " + receiver.previous_block_id + " receiving ")
                    .append($("<code>").text(receiver.data_type)))
        }
        detailsContent.append($("<p>")
            .append($("<strong>").text("Receives data from: "))
            .append(list));
    }
};

const buildJobGraph = (structures) => {
    const byBlockId = {};
    for (const entry of structures) {
        const [coord, structure] = entry;
        byBlockId[coord["block_id"]] = structure;
    }
    console.log("byBlockId", byBlockId)
    const nodes = [];
    const links = [];
    const receivers = {};

    const operatorId = (block_id, index) => {
        return 100000 + block_id * 1000 + index;
    };

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
                        onclick: () => drawOperatorDetails(operator)
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
                        text: operator.out_type
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
                        text: connection.data_type
                    }
                })
            }
        });
    }

    console.log(receivers);
    return [nodes, links];
};

// var nodes = [
//     {
//         id: 1,
//         data: {
//             text: "Block 0"
//         },
//         children: [
//             {
//                 id: 2,
//                 data: {
//                     text: "Source"
//                 },
//             },
//             {
//                 id: 3,
//                 data: {
//                     text: "Map"
//                 }
//             }
//         ]
//     },
//     {
//         id: 4,
//         data: {
//             text: "Block 1"
//         },
//         children: [
//             {
//                 id: 5,
//                 data: {
//                     text: "StartBlock"
//                 }
//             },
//             {
//                 id: 6,
//                 data: {
//                     text: "Map"
//                 }
//             },
//             {
//                 id: 7,
//                 data: {
//                     text: "Sink"
//                 }
//             }
//         ]
//     }
// ];

// var links = [
//     {
//         source: 2,
//         target: 3,
//         data: {
//             type: "solid",
//             width: 3,
//             text: "woooow"
//         }
//     },
//     {
//         source: 3,
//         target: 5,
//         data: {
//             type: "dashed",
//             text: "nice"
//         }
//     },
//     {
//         source: 5,
//         target: 6,
//         data: {
//             type: "solid",
//             text: "ok"
//         }
//     },
//     {
//         source: 6,
//         target: 7,
//         data: {
//             type: "solid"
//         }
//     },
// ]