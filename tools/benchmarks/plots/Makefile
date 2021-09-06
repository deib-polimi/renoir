TARGET = target
RESULTS = ../results

EXPERIMENTS = \
	car-accidents \
	connected-components \
	enum-triangles \
	kmeans \
	kmeans-noflink \
	pagerank \
	transitive-closure \
	wordcount-gutenberg wordcount-randomwords \
	wordcount-assoc-gutenberg \
	wordcount-windowed-gutenberg wordcount-windowed-randomwords

DEP_car-accidents := $(RESULTS)/car-accidents*
TITLE_car-accidents := "Car Accidents"
BASE_car-accidents := car-accidents
EXTRA_car-accidents := car-accidents-shared
EXTRANAME_car-accidents := shared

DEP_connected-components := $(RESULTS)/connected-components
TITLE_connected-components := "Connected Components"
BASE_connected-components := connected-components

DEP_enum-triangles := $(RESULTS)/enum-triangles/{flink,rstream2} $(RESULTS)/enum-triangles-new
TITLE_enum-triangles := "Enum Triangles"
BASE_enum-triangles := enum-triangles

DEP_kmeans := $(RESULTS)/kmeans-30c-15it-200M
TITLE_kmeans := "K-Means (k = 30, h = 15, n = 10M)"
BASE_kmeans := kmeans

DEP_kmeans-noflink := $(RESULTS)/kmeans-30c-15it-200M/{mpi,rstream*}
TITLE_kmeans-noflink := "K-Means (k = 30, h = 15, n = 10M)"
BASE_kmeans-noflink := kmeans

DEP_pagerank := $(RESULTS)/pagerank
TITLE_pagerank := "PageRank"
BASE_pagerank := pagerank

DEP_transitive-closure := $(RESULTS)/transitive-closure
TITLE_transitive-closure := "Transitive Closure"
BASE_transitive-closure := transitive-closure

DEP_wordcount-gutenberg := $(RESULTS)/wordcount/{rstream2,timely} $(RESULTS)/wordcount-fixed-new
TITLE_wordcount-gutenberg := "Non-associative Wordcount (Gutenberg)"
BASE_wordcount-gutenberg := wordcount

DEP_wordcount-randomwords := $(RESULTS)/wordcount-randomwords
TITLE_wordcount-randomwords := "Non-associative Wordcount (Randomwords)"
BASE_wordcount-randomwords := wordcount

DEP_wordcount-assoc-gutenberg := $(RESULTS)/wordcount-assoc
TITLE_wordcount-assoc-gutenberg := "Associative Wordcount (Gutenberg)"
BASE_wordcount-assoc-gutenberg := wordcount-assoc

DEP_wordcount-windowed-gutenberg := $(RESULTS)/wordcount-windowed/rstream2 $(RESULTS)/wordcount-windowed/mpi $(RESULTS)/wordcount-windowed/flink $(RESULTS)/wordcount-windowed-new
TITLE_wordcount-windowed-gutenberg := "Windowed Wordcount (Gutenberg)"
BASE_wordcount-windowed-gutenberg := wordcount

DEP_wordcount-windowed-randomwords := $(RESULTS)/wordcount-windowed-randomwords/rstream2 $(RESULTS)/wordcount-windowed-randomwords/mpi $(RESULTS)/wordcount-windowed-randomwords/flink $(RESULTS)/wordcount-windowed-randomwords-fixed
TITLE_wordcount-windowed-randomwords := "Windowed Wordcount (Randomwords)"
BASE_wordcount-windowed-randomwords := wordcount

PDF = $(EXPERIMENTS:%=$(TARGET)/%/time.pdf) $(EXPERIMENTS:%=$(TARGET)/%/scaling.pdf)

all: $(PDF)

$(TARGET)/%/:
	mkdir -p $@

$(TARGET)/%/time.pdf: $(TARGET)/%/
	../gen_csv.py $(DEP_$*) | ../make_plot.py time -o $@ \
		-t $(TITLE_$*) \
		--base-experiment "$(BASE_$*)" \
		--extra-experiment "$(EXTRA_$*)" \
		--extra-experiment-name "$(EXTRANAME_$*)"

$(TARGET)/%/scaling.pdf: $(TARGET)/%/
	../gen_csv.py $(DEP_$*) | ../make_plot.py scaling -o $@ \
		--ideal \
		-t $(TITLE_$*) \
		--base-experiment "$(BASE_$*)" \
		--extra-experiment "$(EXTRA_$*)" \
		--extra-experiment-name "$(EXTRANAME_$*)"

clean:
	rm -rf $(TARGET)

.PHONY: all clean
.PRECIOUS: $(TARGET)/%/
