/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Test.Recommender;

import Database.Primitives.User;
import Database.Songs;
import Main.KNNOptions;
import Main.Main;
import Recommender.SequentialKNN;
import java.io.PrintStream;
import org.easymock.EasyMock;
import org.junit.*;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith( PowerMockRunner.class )
@PrepareForTest( Main.class )
/**
 *
 * @author sarah
 */
public class SequentialKNNTest {
    
    public SequentialKNNTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of createNeighborhoods method, of class SequentialKNN.
     */
    @Test
    public void testCreateNeighborhoods() {
        System.out.println("createNeighborhoods");

        // TODO review the generated test code and remove the default call to fail.
        
        KNNOptions knnOptions = new KNNOptions();
        CmdLineParser parser = new CmdLineParser(knnOptions);
        try {
            String[] args = new String[3]; 
            args[0] = "-k";
            args[1] = "3";
            args[2] = "smalltest.txt";
            parser.parseArgument(args);
        } catch (CmdLineException ex) {
            System.out.println("Unable to parse args for test.");
            System.out.println(ex);
        }
        PowerMock.mockStatic(Main.class);
        EasyMock.expect(Main.getOptions()).andReturn(knnOptions);
        //KNNOptions mockOptions = EasyMock.createMock(KNNOptions.class);
        //EasyMock.expect(mockOptions.getDatabasePath()).andReturn("smalltest.txt");
        PrintStream s = EasyMock.createMock(PrintStream.class);
        s.println("Fill this in with the stuff we expect");
        //EasyMock.replay(s);      
        PowerMock.replayAll();
        //EasyMock.replay(mockOptions);
        SequentialKNN instance = new SequentialKNN();
        instance.createNeighborhoods();
        //EasyMock.verify(s);
        PowerMock.verifyAll();
        
    }

    /**
     * Test of recommendSong method, of class SequentialKNN.
     */
    @Test
    public void testRecommendSong() {
        System.out.println("recommendSong");
        User active = null;
        Songs songs = null;
        double threshold = 0.0;
        SequentialKNN instance = new SequentialKNN();
        instance.recommendSong(active, songs, threshold);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}
