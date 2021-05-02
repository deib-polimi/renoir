const inputSelector = document.getElementById("file-picker");
const jobGraphRadio = document.getElementById("job-graph");
const executionGraphRadio = document.getElementById("execution-graph");
const gravitySelect = document.getElementById("gravity");
const redrawButton = document.getElementById("redraw");

window.graphMode = "job";
window.profiler = null;

const drawGraph = (structures, profiler) => {
    if (window.graphMode === "job") {
        drawJobGraph(structures, profiler);
    } else {
        drawExecutionGraph(structures, profiler);
    }
}

inputSelector.addEventListener("change", (event) => {
    const files = event.target.files;
    if (files.length !== 1) {
        return;
    }
    const file = files[0];
    const reader = new FileReader();
    reader.addEventListener("load", (event) => {
        const content = event.target.result;
        let json;
        try {
            json = JSON.parse(content);
        } catch (e) {
            alert("Malformed JSON data: " + e);
        }
        window.structures = json["structures"];
        window.profilers = json["profilers"];
        if (!window.structures || !window.profilers) {
            alert("Invalid JSON data: structures or profilers missing");
            return;
        }
        window.profiler = new Profiler(window.profilers);
        drawGraph(window.structures, window.profiler);
    });
    reader.readAsText(file);
});

jobGraphRadio.addEventListener("change", () => {
    window.graphMode = "job";
    if (window.profiler) {
        drawGraph(window.structures, window.profiler);
    }
});

executionGraphRadio.addEventListener("change", () => {
    window.graphMode = "execution";
    if (window.profiler) {
        drawGraph(window.structures, window.profiler);
    }
});

gravitySelect.addEventListener("change", () => {
    changeGravity(!gravityEnabled);
});

redrawButton.addEventListener("click", () => {
    if (window.profiler) {
        drawGraph(window.structures, window.profiler);
    }
});