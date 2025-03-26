#include <iostream>

#include "joiner.h"
#include "parser.h"

using namespace std;

int main(int argc, char *argv[]) {
    Joiner joiner(THREAD_NUM);

    // Read join relations
    std::string line;
    while (getline(std::cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    // Preparation phase (not timed)
    // Build histograms, indexes,...

    QueryInfo i;
    while (getline(std::cin, line)) {
        if (line == "F") { // End of a batch
            joiner.waitAsyncJoins();
            auto results = joiner.getAsyncJoinResults(); // result strings vector
            for (auto& result : results)
                cout << result;
            continue;
        } 
        joiner.createAsyncQueryTask(line);
    }

    return 0;
}
