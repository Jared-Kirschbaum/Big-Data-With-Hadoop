package csx55.hadoop.common;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Dataframe implements Writable {
    public int count;
    public double totalLoudness; // added for Q2
    public double songHotness; // For Q3
    public String songId = ""; // Added for Tracking
    public double fadeIn; // Added for Q4
    public double duration; // added for Q5
    public double danceability; // added for Q6
    public double energy; // added for Q6


    public void set(int count, double totalLoudness, double songHotness, String songId, double fadeIn, double duration, double danceability, double energy) {
        this.count = count;
        this.totalLoudness = totalLoudness;
        this.songHotness = songHotness;
        this.songId = songId;
        this.fadeIn = fadeIn;
        this.duration = duration;
        this.danceability = danceability;
        this.energy = energy;
    }

    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeDouble(totalLoudness);
        out.writeDouble(songHotness);
        out.writeUTF(songId);
        out.writeDouble(fadeIn);
        out.writeDouble(duration);
        out.writeDouble(danceability);
        out.writeDouble(energy);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        totalLoudness = in.readDouble();
        songHotness = in.readDouble();
        songId = in.readUTF(); // Read songId as UTF
        fadeIn = in.readDouble();
        duration = in.readDouble();
        danceability = in.readDouble();
        energy = in.readDouble();
    }
}
