#include <iostream>

#include "relation.hpp"
#include "parser.hpp"

int main(int argc, char *argv[]) {
    std::cout << "Transforms our query format to SQL" << std::endl;

    QueryInfo i;
    for (std::string line; std::getline(std::cin, line);) {
        i.parseQuery(line);
        std::cout << i.dumpSQL() << std::endl;
    }

    return 0;
}

