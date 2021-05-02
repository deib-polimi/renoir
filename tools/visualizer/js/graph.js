const BUCKET_RESOLUTION = 20;
const BUCKET_MERGE_FACTOR = 5;

const graphContainerId = "graph-content";

const resetGraph = () => {
    d3.select("#" + graphContainerId).select("svg").remove();
};

const drawProfilerGraph = (series, title, iteration_boundaries, yFormat) => {
    if (!yFormat) yFormat = (d) => d;

    const iterToIndex = {};
    let maxIterTime = 0;
    Object.entries(iteration_boundaries.data).map(([block_id, times], index) => {
        iterToIndex[block_id] = index;
        maxIterTime = Math.max(maxIterTime, times[times.length - 1]);
    });

    const container = document.getElementById(graphContainerId);
    const svgWidth = container.clientWidth;
    const svgHeight = container.clientHeight;
    const [left, right, top, bottom] = [60, 10, 20, 30];

    resetGraph();
    const svg = d3.select("#" + graphContainerId)
        .append("svg")
        .attr("width", svgWidth)
        .attr("height", svgHeight);

    const root = svg.append("g");

    const rawData = series.asList();
    const data = [];
    const scaleFactor = 1000 / BUCKET_RESOLUTION / BUCKET_MERGE_FACTOR;
    let bucketTime = 0;
    let bucketValue = 0;
    for (let i = 0; i < rawData.length; i++) {
        const [time, value] = rawData[i];
        if (time < bucketTime + BUCKET_RESOLUTION * BUCKET_MERGE_FACTOR) {
            bucketValue += value;
        } else {
            if (bucketValue > 0)
                data.push([bucketTime, bucketValue * scaleFactor]);
            bucketTime = time;
            bucketValue = value;
        }
    }
    data.push([bucketTime, bucketValue * scaleFactor]);

    const x = d3.scaleLinear()
        .domain([0, Math.max(d3.max(data, (d) => d[0]), maxIterTime)])
        .range([left, svgWidth-right]);
    // x-axis
    root
        .append("g")
        .attr("transform", `translate(0,${svgHeight - bottom})`)
        .call(d3.axisBottom(x).ticks(5));
    // vertical grid
    root
        .append("g")
        .attr("transform", `translate(0,${svgHeight - bottom})`)
        .attr("class", "grid")
        .call(d3
            .axisBottom(x)
            .tickSize(-(svgHeight - bottom - top))
            .tickFormat("")
            .ticks(5));
    for (const [block_id, times] of Object.entries(iteration_boundaries.data)) {
        const color = d3.schemeCategory20[iterToIndex[block_id]];
        for (const time of times) {
            const xPos = x(time);
            root.append("line")
                .attr("stroke", color)
                .attr("x1", xPos)
                .attr("x2", xPos)
                .attr("y1", top)
                .attr("y2", svgHeight-bottom);
        }
    }

    const y = d3.scaleLinear()
        .domain([0, d3.max(data, (d) => d[1])])
        .range([svgHeight - bottom, top]);
    // y-axis
    root
        .append("g")
        .attr("transform", `translate(${left},0)`)
        .call(d3.axisLeft(y).ticks(10).tickFormat(yFormat));
    // horizontal grid
    root
        .append("g")
        .attr("transform", `translate(${left},0)`)
        .attr("class", "grid")
        .call(d3
            .axisLeft(y)
            .tickSize(-(svgWidth - left - right))
            .tickFormat("")
            .ticks(10));

    root.append("path")
        .datum(data)
        .attr("fill", "none")
        .attr("stroke", "blue")
        .attr("stroke-width", 1.5)
        .attr("d", d3.line()
            .x((d) => x(d[0]))
            .y((d) => y(d[1])))

    root
        .append("text")
        .attr("text-anchor", "middle")
        .attr("alignment-baseline", "middle")
        .attr("x", svgWidth / 2)
        .attr("y", top / 2)
        .text(title);
}