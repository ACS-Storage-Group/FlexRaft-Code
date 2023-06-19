
#include <algorithm>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "DemoInterface.hpp"

#include <RCF/RCF.hpp>

int main()
{
    RCF::RcfInit rcfInit;

    try
    {

		std::string networkInterface = "127.0.0.1";
		int port = 50001;
		std::cout << "Connecting to server on " << networkInterface << ":" << port << "." << std::endl;

        // Setup a vector of strings.
        std::vector<std::string> v;
        v.push_back("one");
        v.push_back("two");
        v.push_back("three");

        // Print them out.
        std::cout << "Before:\n";
        std::copy(
            v.begin(), 
            v.end(), 
            std::ostream_iterator<std::string>(std::cout, "\n"));

        // Make the call.
        RcfClient<I_DemoService>( RCF::TcpEndpoint(networkInterface, port) ).Reverse(v);

        // Print them out again. This time they are in reverse order.
        std::cout << "\nAfter:\n";
        std::copy(
            v.begin(), 
            v.end(), 
            std::ostream_iterator<std::string>(std::cout, "\n"));
    }
    catch(const RCF::Exception & e)
    {
        std::cout << "Caught exception:\n";
        std::cout << e.getErrorMessage() << std::endl;
        return 1;
    }

    return 0;
}
