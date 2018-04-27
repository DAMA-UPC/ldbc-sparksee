#include "snbLoader.h"
#include <iostream>
#include <clocale>
#include <time.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::wcout << L"usage: snb_test <cmds>" << std::endl;
    std::wcout.flush();
    return 1;
  }


  setenv("TZ", "GMT", 1);
  tzset();
  return snb_loader::Parse(argv[1]) ? 1 : 0;
}
