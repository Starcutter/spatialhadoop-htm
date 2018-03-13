package edu.umn.cs.spatialHadoop.core;

import cn.edu.tsinghua.cs.htm.HTM;
import cn.edu.tsinghua.cs.htm.utils.HTMid;

public class HTMInfo {
    public HTM htm;
    public int num_partitions;

    public HTMInfo(int num_partitions) {
        this.htm = HTM.getInstance();
        this.num_partitions = num_partitions;
    }

    public HTMidInfo[] getAllHTMidInfos() {
        HTMidInfo[] topIds = new HTMidInfo[8];
        for (int i = 0; i < 8; i++) {
            topIds[i] = new HTMidInfo(htm.getTopTrixel(i).getHTMid());
        }
        if (this.num_partitions == 8) {
            return topIds;
        }
        HTMidInfo[] secondIds = new HTMidInfo[32];
        int index = 0;
        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 4; j++) {
                secondIds[index++] = new HTMidInfo(topIds[i].htmId.getChild(j));
            }
        }
        if (this.num_partitions == 32) {
            return secondIds;
        }
        HTMidInfo[] thirdIds = new HTMidInfo[128];
        index = 0;
        for (int i = 0; i < 32; i++) {
            for (int j = 0; j < 4; j++) {
                thirdIds[index++] = new HTMidInfo(secondIds[i].htmId.getChild(j));
            }
        }
        return thirdIds;
    }
}
