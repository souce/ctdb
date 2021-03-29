CC=gcc
BIN = test
BIN_PATH = ./

SRC_PATH = ./
SRC_FILE = $(wildcard $(SRC_PATH)/*.c)
SRC_OBJS = $(patsubst %.c,%.o,$(SRC_FILE))

INC_PATH = -I$(SDS_PATH) -I$(SRC_PATH)
CFLAGS = -g -O0 -Wall $(INC_PATH)

$(BIN): $(SRC_OBJS) 
	$(CC) -o $(BIN_PATH)/$@ $(SDS_OBJS) $(SRC_OBJS) $(CFLAGS)
	@echo "compile $(BIN) success!";

clean:
	$(RM) $(AE_PATH)/*.o $(SDS_PATH)/*.o $(PHR_PATH)/*.o $(INIH_PATH)/*.o $(COM_PATH)/*.o $(CO_PATH)/*.o $(SRC_PATH)/*.o $(BIN_PATH)/$(BIN)
