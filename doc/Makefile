TARGET = sgas_aggregator

FILES =$(TARGET).tex intro.tex
#FILES+= 

GRAPH = figure

INPUT = $(GRAPH) $(FILES)

ps: $(TARGET).ps

pdf: $(TARGET).pdf


$(TARGET).ps: $(INPUT)
	latex $(TARGET).tex
	latex $(TARGET).tex
	dvips -o $(TARGET).ps -t a4 $(TARGET).dvi

$(TARGET).pdf: $(TARGET).ps
	ps2pdf -sPAPERSIZE=a4 $(TARGET).ps $(TARGET).pdf
#	distill -pagesize 21.0 29.7 cm $(TARGET).ps


clean:
	-rm *.aux *.log *.dvi *.*~ *.bak

proper: clean
	-rm $(TARGET).ps $(TARGET).pdf


