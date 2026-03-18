package ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl_client;

import com.squareup.moshi.Moshi;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.logging.HttpLoggingInterceptor;
import okio.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.moshi.MoshiConverterFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

public class RetrofitExecutor {
    private static final Logger log = LoggerFactory.getLogger(RetrofitExecutor.class);
    private final Retrofit retrofit;

    public RetrofitExecutor(String url) {
        final HttpLoggingInterceptor loggingInterceptor = new HttpLoggingInterceptor()
                .setLevel(HttpLoggingInterceptor.Level.BASIC);
        final OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .readTimeout(100, TimeUnit.SECONDS)
                .connectTimeout(100, TimeUnit.SECONDS)
                .addInterceptor(loggingInterceptor)
                .build();
        final MoshiConverterFactory moshiConverterFactory = MoshiConverterFactory.create(new Moshi.Builder().build());
        retrofit = new Retrofit.Builder()
                .baseUrl(url)
                .client(okHttpClient)
                .addConverterFactory(moshiConverterFactory)
                .build();
    }

    public <T> T createService(Class<T> serviceClass) {
        return retrofit.create(serviceClass);
    }

    public <T> CustomResponse<T> execute(Call<T> call) {
        try {
            final Response<T> response = call.execute();
            final int code = response.code();
            if (response.isSuccessful()) {
                final T data = response.body();
                return CustomResponse.success(data, code);
            } else {
                final String errorMessage = response.errorBody() != null ? response.errorBody().string() : null;
                final String uri = call.request().url().toString();
                final String method = call.request().method();
                final String requestBody = requestBody(call.request());
                log.error("Http response code = {}, errorMessage = `{}`, while executing request URI = {}, method = {}, requestBody = `{}`",
                        code,
                        errorMessage,
                        uri,
                        method,
                        requestBody);
                return CustomResponse.error(code, errorMessage, uri, method, requestBody);
            }
        } catch (IOException e) {
            log.error("Ошибка выполнения запроса по URL {} ", call.request().url(), e);
            throw new UncheckedIOException(e);
        }
    }

    private String requestBody(Request request) throws IOException {
        final RequestBody body = request.body();
        if (body == null) {
            return null;
        }
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Buffer sink = new Buffer();
        body.writeTo(sink);
        sink.copyTo(out);
        sink.flush();
        return out.toString();
    }
}
