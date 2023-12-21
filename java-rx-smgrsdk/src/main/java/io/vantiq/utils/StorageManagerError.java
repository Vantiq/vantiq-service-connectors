package io.vantiq.utils;

/*
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */

import lombok.Value;

@Value
public class StorageManagerError {
    String errorCode;
    String errorMessage;
}
