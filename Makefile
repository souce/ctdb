CC=gcc
BIN = test
BIN_PATH = ./
EXAMPLE_PATH = ./example

SRC_PATH = ./
SRC_FILE = $(wildcard $(SRC_PATH)/*.c)
SRC_OBJS = $(patsubst %.c,%.o,$(SRC_FILE))

INC_PATH = -I$(SRC_PATH)
CFLAGS = -g -O0 -Wall -D _FILE_OFFSET_BITS=64 $(INC_PATH)

clean:
	$(RM) $(SRC_PATH)/*.o $(BIN_PATH)/simple $(BIN_PATH)/trans $(BIN_PATH)/iter

simple: $(SRC_OBJS) 
	$(CC) -o $(BIN_PATH)/$@ $(SRC_OBJS) $(EXAMPLE_PATH)/simple.c $(CFLAGS)
	@echo "compile '$@' success!";

trans: $(SRC_OBJS) 
	$(CC) -o $(BIN_PATH)/$@ $(SRC_OBJS) $(EXAMPLE_PATH)/trans.c $(CFLAGS)
	@echo "compile '$@' success!";

iter: $(SRC_OBJS) 
	$(CC) -o $(BIN_PATH)/$@ $(SRC_OBJS) $(EXAMPLE_PATH)/iter.c $(CFLAGS)
	@echo "compile '$@' success!";
