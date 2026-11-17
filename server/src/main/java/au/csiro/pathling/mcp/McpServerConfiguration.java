/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.csiro.pathling.mcp;

import au.csiro.pathling.PathlingServerVersion;
import au.csiro.pathling.config.ServerConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpStatelessSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStatelessServerTransport;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import jakarta.annotation.Nonnull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Serves the Model Context Protocol (MCP) at {@code /mcp}, using the stateless Streamable HTTP
 * transport, so that any server replica can answer any request.
 *
 * <p>Tool calls execute on the servlet thread, where the security context established by the filter
 * chain is available to the authority checks.
 *
 * @author John Grimes
 * @see <a href="https://modelcontextprotocol.io/specification/2025-06-18/basic/transports">MCP
 *     transports</a>
 */
@Configuration
@ConditionalOnProperty(prefix = "pathling", name = "mcp.enabled", havingValue = "true")
public class McpServerConfiguration {

  private static final String INSTRUCTIONS =
      """
      This server gives read-only access to the FHIR data held by a Pathling server. Use \
      list_resource_types to learn which resource types hold data. Use read_resource and search \
      to look at individual resources. To count, aggregate or tabulate, flatten resources with a \
      SQL on FHIR ViewDefinition using run_view_definition, or join and aggregate views with SQL \
      using run_sql_query, rather than paging through search results. Stored ViewDefinitions can \
      be found by searching the ViewDefinition resource type.\
      """;

  /**
   * Creates the transport, which is the servlet that receives MCP requests.
   *
   * @return the transport
   */
  @Bean
  public HttpServletStatelessServerTransport mcpTransport() {
    return HttpServletStatelessServerTransport.builder()
        .jsonMapper(jsonMapper())
        .messageEndpoint(McpResourceUrl.ENDPOINT_PATH)
        .build();
  }

  /**
   * Registers the transport servlet at {@code /mcp}.
   *
   * @param transport the transport
   * @return the servlet registration
   */
  @Bean
  public ServletRegistrationBean<HttpServletStatelessServerTransport> mcpServlet(
      @Nonnull final HttpServletStatelessServerTransport transport) {
    final ServletRegistrationBean<HttpServletStatelessServerTransport> registration =
        new ServletRegistrationBean<>(transport, McpResourceUrl.ENDPOINT_PATH);
    registration.setName("mcp");
    return registration;
  }

  /**
   * Creates the MCP server, which answers requests arriving through the transport.
   *
   * @param transport the transport
   * @param configuration the server configuration
   * @param executor executes the tools
   * @param version the server version, reported to clients
   * @return the MCP server
   */
  @Bean(destroyMethod = "close")
  public McpStatelessSyncServer mcpServer(
      @Nonnull final HttpServletStatelessServerTransport transport,
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final McpToolExecutor executor,
      @Nonnull final PathlingServerVersion version) {
    return McpServer.sync(transport)
        .serverInfo("Pathling", version.getBuildVersion().orElse("unknown"))
        .capabilities(ServerCapabilities.builder().tools(false).build())
        .instructions(INSTRUCTIONS)
        .tools(new McpTools(configuration, executor).specifications())
        .jsonMapper(jsonMapper())
        // The tool runs on the servlet thread, where the security context is held.
        .immediateExecution(true)
        .build();
  }

  /**
   * Creates the JSON mapper for MCP messages. The transport serialises the exception it answers a
   * malformed request with, so the internals of an exception are left out of the response.
   */
  @Nonnull
  private static McpJsonMapper jsonMapper() {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.addMixIn(Throwable.class, ThrowableMixIn.class);
    return new JacksonMcpJsonMapper(mapper);
  }

  /** Leaves the stack trace, cause and suppressed exceptions out of a serialised exception. */
  @JsonIgnoreProperties({"stackTrace", "cause", "suppressed", "localizedMessage"})
  private abstract static class ThrowableMixIn {}
}
