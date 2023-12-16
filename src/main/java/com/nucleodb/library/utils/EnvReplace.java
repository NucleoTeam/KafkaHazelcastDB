package com.nucleodb.library.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnvReplace{

    public static String replaceEnvVariables(String input) {
        // Regular expression to find patterns like ${ENVARNAME:default_value}
        Pattern pattern = Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?\\}");
        Matcher matcher = pattern.matcher(input);

        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            // Extract the environment variable name and the default value
            String envVarName = matcher.group(1);
            String defaultValue = matcher.group(2) == null ? "" : matcher.group(2);

            // Get the environment variable value, use default value if not set
            String envVarValue = System.getenv(envVarName);
            if (envVarValue == null) {
                envVarValue = defaultValue;
            }

            // Replace the pattern with the value
            matcher.appendReplacement(result, envVarValue);
        }

        matcher.appendTail(result);

        return result.toString();
    }
}