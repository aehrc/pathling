package au.csiro.pathling.library.io.sink;

import au.csiro.pathling.io.source.DataSource;

/**
 * Encapsulates changes caused by the {@link au.csiro.pathling.library.io.sink.DataSink#write(DataSource)}
 * on the filesystem. Subclasses provide more details.
 * 
 * @author Felix Naumann
 */
public sealed interface WriteDetails permits NdjsonWriteDetails {

}
