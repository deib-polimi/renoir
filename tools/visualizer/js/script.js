const inputSelector = document.getElementById("file-picker");

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
        const structures = json["structures"];
        const profilers = json["profilers"];
        if (!structures || !profilers) {
            alert("Invalid JSON data: structures or profilers missing");
            return;
        }
        processData(structures, profilers);
    });
    reader.readAsText(file);
});