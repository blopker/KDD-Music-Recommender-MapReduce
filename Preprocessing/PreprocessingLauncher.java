/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Preprocessing;

/**
 *
 * @author sarah
 */
public class PreprocessingLauncher {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: <inFile> <outFilePrefix> <numberOfChunks> <numberOfSongs>");
            System.exit(1);
        }
        Preprocessing pp = new Preprocessing(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        pp.parse();
        pp.close();
    }
}
