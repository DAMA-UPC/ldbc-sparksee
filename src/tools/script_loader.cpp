

#include "script/ScriptParser.h"
#include <iostream>

int main(int argc, char *argv[]) {

	std::string str(argv[1]);
	sparksee::script::ScriptParser parser;
	parser.Parse(std::wstring(str.begin(), str.end()),true,L"");
	return 0;
}



