/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import edu.umn.cs.spatialHadoop.core.HTMPoint;
import edu.umn.cs.spatialHadoop.core.HTMidInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import edu.umn.cs.spatialHadoop.core.SpatialSite;

public class HTMOutputFormat extends FileOutputFormat<IntWritable, HTMPoint> {

    @Override
    public RecordWriter<IntWritable, HTMPoint> getRecordWriter(FileSystem ignored,
                                                                JobConf job,
                                                                String name,
                                                                Progressable progress)
            throws IOException {
        HTMidInfo[] htmIdInfos = SpatialSite.getHTMids(job);
        HTMRecordWriter writer = new HTMRecordWriter(job, name, htmIdInfos);
        return writer;
    }

}

