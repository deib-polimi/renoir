class TimeSeries {
    constructor() {
        this.data = {};
        this.total = 0;
    }

    add(bucket, value) {
        if (!(bucket in this.data)) this.data[bucket] = 0;
        this.data[bucket] += value;
        this.total += value;
    }

    asList() {
        const res = Object.entries(this.data);
        return res
            .sort(([a,], [b,]) => a - b)
            .map(([t, v]) => [+t, +v]);
    }

    static merge(a, b) {
        const result = new TimeSeries();
        for (const [bucket, value] of Object.entries(a.data)) {
            result.data[bucket] = value;
        }
        for (const [bucket, value] of Object.entries(b.data)) {
            if (!(bucket in result.data)) result.data[bucket] = 0;
            result.data[bucket] += value;
        }
        result.total = a.total + b.total;
        return result;
    }
}

class ChannelMetric {
    constructor() {
        this.data = {};
    }

    add(from, to, bucket, value) {
        const key = ChannelMetric.coordPairKey(from, to);
        if (!(key in this.data)) {
            this.data[key] = {
                from,
                to,
                series: new TimeSeries()
            };
        }
        this.data[key].series.add(bucket, value);
    }

    groupByBlockId() {
        const byBlockId = {};
        for (const {from, to, series} of Object.values(this.data)) {
            const key = ChannelMetric.blockPairKey(from.block_id, to.block_id);
            if (!(key in byBlockId)) byBlockId[key] = {from, to, series: new TimeSeries()};
            byBlockId[key].series = TimeSeries.merge(byBlockId[key].series, series);
        }
        return byBlockId;
    }

    static blockPairKey(from, to) {
        return `${from}:${to}`;
    }

    static coordPairKey(from, to) {
        return `${this.coordKey(from)}|${this.coordKey(to)}`;
    }

    static coordKey(coord) {
        return `${coord.host_id}:${coord.block_id}:${coord.replica_id}`;
    }
}

class IterationBoundaries {
    constructor() {
        this.data = {};
    }

    add(block_id, time) {
        if (!(block_id in this.data)) this.data[block_id] = [];
        this.data[block_id].push(time);
    }
}

class Profiler {
    constructor(data) {
        this.channel_metrics = {
            items_in: new ChannelMetric(),
            items_out: new ChannelMetric(),
            net_messages_in: new ChannelMetric(),
            net_messages_out: new ChannelMetric(),
            net_bytes_in: new ChannelMetric(),
            net_bytes_out: new ChannelMetric(),
        };
        this.iteration_boundaries = new IterationBoundaries();
        for (const profiler of data) {
            for (const {metrics, start_ms} of profiler.buckets) {
                for (const {from, to, value} of metrics.items_in) {
                    this.channel_metrics.items_in.add(from, to, start_ms, value);
                }
                for (const {from, to, value} of metrics.items_out) {
                    this.channel_metrics.items_out.add(from, to, start_ms, value);
                }
                for (const {from, to, value} of metrics.net_messages_in) {
                    this.channel_metrics.net_messages_in.add(from, to, start_ms, value[0]);
                    this.channel_metrics.net_bytes_in.add(from, to, start_ms, value[1]);
                }
                for (const {from, to, value} of metrics.net_messages_out) {
                    this.channel_metrics.net_messages_out.add(from, to, start_ms, value[0]);
                    this.channel_metrics.net_bytes_out.add(from, to, start_ms, value[1]);
                }
                for (const [block_id, time] of metrics.iteration_boundaries) {
                    this.iteration_boundaries.add(block_id, time);
                }
            }
        }
    }
}