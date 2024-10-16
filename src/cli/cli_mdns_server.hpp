/*
 *  Copyright (c) 2023, The OpenThread Authors.
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the copyright holder nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file
 *   This file contains definitions for a simple CLI to control MDNS Server.
 */

#ifndef CLI_MDNS_SERVER_CPP_
#define CLI_MDNS_SERVER_CPP_

#include "openthread-core-config.h"

#include <openthread/mdns_server.h>

#include "cli/cli_config.h"
#include "cli/cli_output.hpp"

#if OPENTHREAD_CONFIG_MDNS_SERVER_ENABLE

namespace ot {
namespace Cli {

/**
 * This class implements the MDNS Server CLI interpreter.
 *
 */
class MdnsServer : private Output
{
public:
    typedef Utils::CmdLineParser::Arg Arg;

    /**
     * Constructor
     *
     * @param[in]  aInstance            The OpenThread Instance.
     * @param[in]  aOutputImplementer   An `OutputImplementer`.
     *
     */
    MdnsServer(otInstance *aInstance, OutputImplementer &aOutputImplementer);

    /**
     * This method interprets a list of CLI arguments.
     *
     * @param[in]  aArgs        A pointer an array of command line arguments.
     *
     */
    otError Process(Arg aArgs[]);

private:
    static constexpr uint8_t kIndentSize = 4;
    using Command                        = CommandEntry<MdnsServer>;

    template <CommandId kCommandId> otError Process(Arg aArgs[]);

    void OutputHostAddresses(void);
};

} // namespace Cli
} // namespace ot

#endif // OPENTHREAD_CONFIG_MDNS_SERVER_ENABLE

#endif // CLI_MDNS_SERVER_CPP_
