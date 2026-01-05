package au.csiro.pathling.library.io.sink;

import java.util.List;

/**
 * Capture changes made by the write operation methods on the filesystem.
 *
 * @param fileInfos A list of files that have been created/modified on the filesystem.
 * @author Felix Naumann
 */
public record WriteDetails(List<FileInformation> fileInfos) {}
