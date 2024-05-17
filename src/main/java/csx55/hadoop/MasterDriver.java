package csx55.hadoop;

import csx55.hadoop.q1.MaxSongDriver;
import csx55.hadoop.q2.LoudestSongDriver;
import csx55.hadoop.q3.HottestSongDriver;
import csx55.hadoop.q4.LongestFadeDriver;
import csx55.hadoop.q5.SongDurationDriver;
import csx55.hadoop.q6.EnergyDanceDriver;
import csx55.hadoop.q7.SegmentDriver;
import csx55.hadoop.q8.UniqueDriver;
import csx55.hadoop.q9.NewHottestDriver;
import csx55.hadoop.q10.RecommendationDriver;

public class MasterDriver {
    public static void main(String[] args) throws Exception {
        String[] songCountArgs = {"/ms/metadata.txt", "/ms/q1/job_one", "/ms/q1/final_answer"};
        String[] loudnessArgs = {"/ms/analysis.txt", "/ms/metadata.txt", "/ms/q2/job_one", "/ms/q2/job_two", "/ms/q2/final_answer"};
        String[] hottestSongArgs = {"/ms/analysis.txt", "/ms/q3/final_answer"};
        String[] fadeArgs = {"/ms/analysis.txt", "/ms/metadata.txt", "/ms/q4/job_one", "/ms/q4/job_two", "/ms/q4/final_answer"};
        String[] songLengthArgs = {"/ms/analysis.txt","/ms/q5/job_one", "/ms/q5/final_answer"};
        String[] energyDanceArgs = {"/ms/analysis.txt","/ms/q6/final_answer"};
        String[] segmentArgs = {"/ms/analysis.txt","/ms/q7/job_one", "/ms/q7/job_two", "/ms/q7/final_answer"};
        String[] uniqueArgs = {"/ms/analysis.txt", "/ms/metadata.txt","/ms/q8/job_one", "/ms/q8/job_two", "/ms/q8/job_three", "/ms/q8/final_answer"};
        String[] newHottestArgs = {"/ms/analysis.txt", "/ms/metadata.txt","/ms/q9/job_one", "/ms/q9/job_two", "/ms/q9/job_three", "/ms/q9/final_answer", "/ms/q9/terms", "/ms/q9/global_top_terms"};
        String[] recArgs = {"/ms/analysis.txt", "/ms/metadata.txt","/ms/q10/job_one", "/ms/q10/job_two", "/ms/q10/job_three", "/ms/q10/job_four", "/ms/q10/job_five", "punk rock hop 40s germany ambient urban world bongo banda", "/ms/q10/IDF", "/ms/q10/playlist"};

        // Q1
        if (!MaxSongDriver.main(songCountArgs)) {
            System.out.println("Max Song Count Job failed.");
            return;
        }


        // Q2
        if (!LoudestSongDriver.main(loudnessArgs)) {
            System.out.println("Loudness Calculation Job failed.");
            return;
        }

        // Q3
        if (!HottestSongDriver.main(hottestSongArgs)) {
            System.out.println("Hottness Calculation Job failed.");
            return;
        }

        // Q4
        if (!LongestFadeDriver.main(fadeArgs)) {
            System.out.println("Longest Fade Job Calculation Job failed.");
            return;
        }

        // Q5
        if (!SongDurationDriver.main(songLengthArgs)) {
            System.out.println("Song Duration Job failed.");
            return;
        }


         // Q6
         if (!EnergyDanceDriver.main(energyDanceArgs)) {
            System.out.println("Song Duration Job failed.");
            return;
        }

        // // Q7
         if (!SegmentDriver.main(segmentArgs)) {
            System.out.println("Song Duration Job failed.");
            return;
        }

        //Q8
        if (!UniqueDriver.main(uniqueArgs)) {
            System.out.println("Song Duration Job failed.");
            return;
        }


        //Q9
        if (!NewHottestDriver.main(newHottestArgs)) {
            System.out.println("Song Duration Job failed.");
            return;
        }

        
        //Q10
        if (!RecommendationDriver.main(recArgs)) {
            System.out.println("Song Duration Job failed.");
            return;
        }

        System.out.println("All jobs completed successfully.");
    }

}
