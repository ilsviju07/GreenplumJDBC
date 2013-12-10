package util;

public class ParseParameters
{
    public static String findAttribute (String attributeKey, String[] args)
    {
        String attributeValue = null;
        int i = 0;
        while (i < args.length)
        {
            if (args[i].toLowerCase().equals("-" + attributeKey))
            {
                try
                {
                    attributeValue = new String(args[i + 1]);
                } catch (Exception e) {}
                i++;
                break;
            }
            i++;
        }
        return attributeValue;
    }
    
    public static boolean existsAttribute (String attributeKey, String[] args)
    {
        int i = 0;
        while (i < args.length)
        {
            if (args[i].toLowerCase().equals("-" + attributeKey))
            {
            	return true;
            }
            i++;
        }
        return false;
    }
}
